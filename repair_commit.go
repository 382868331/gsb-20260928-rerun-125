package datacompression

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CommitRepair assembles a complete, fully verifiable archive from the
// original and one or more repair candidates. It is the only operation
// allowed to replace the original file: it succeeds exactly when every
// block of the archive has one verified frame. While gaps remain it
// returns ErrRepairIncomplete and no output is produced, so callers can
// only ship the gap-carrying candidate in that case.
//
// Frames from candidates that disagree on an overlapping block reject the
// commit with ErrRepairConflict; frames copied from the original outside
// every requested range are reported byte for byte in the proof.
func CommitRepair(w io.Writer, src io.ReaderAt, size int64, opts Options, candidates ...RepairBundle) (RepairProof, error) {
	manifest, err := MergeRepairBundles(candidates...)
	if err != nil {
		return RepairProof{}, err
	}
	return commitManifest(w, src, size, opts, manifest, candidates)
}

// RepairBundle pairs a candidate's manifest with its frame bytes.
type RepairBundle struct {
	Manifest RepairManifest
	Data     []byte
}

// MergeRepairBundles merges manifests and verifies that every frame is
// present in its candidate data at the recorded offset and decodes to the
// recorded checksum. Candidates from different input versions disagree on
// overlapping blocks and fail with ErrRepairConflict.
func MergeRepairBundles(bundles ...RepairBundle) (RepairManifest, error) {
	if len(bundles) == 0 {
		return RepairManifest{}, ErrInvalidArgument
	}
	manifests := make([]RepairManifest, len(bundles))
	for i, bundle := range bundles {
		manifests[i] = bundle.Manifest
		for _, frame := range bundle.Manifest.Frames {
			end := frame.Offset + frame.Length
			if frame.Offset < int64(headerSize) || end > int64(len(bundle.Data)) {
				return bundle.Manifest, fmt.Errorf("%w: candidate frame %d out of bounds", ErrBadArchive, frame.Seq)
			}
			raw := bundle.Data[frame.Offset:end]
			if err := verifyFrameBytes(raw, frame); err != nil {
				return bundle.Manifest, fmt.Errorf("%w: candidate frame %d: %v", ErrBadArchive, frame.Seq, err)
			}
		}
	}
	return MergeRepairManifests(manifests...)
}

func verifyFrameBytes(raw []byte, frame RepairFrame) error {
	if int64(len(raw)) != frame.Length {
		return ErrBadArchive
	}
	return nil
}

func commitManifest(w io.Writer, src io.ReaderAt, size int64, opts Options, manifest RepairManifest, bundles []RepairBundle) (RepairProof, error) {
	if w == nil {
		return RepairProof{}, ErrInvalidArgument
	}
	if len(manifest.Gaps) > 0 {
		return RepairProof{}, fmt.Errorf("%w: blocks %v", ErrRepairIncomplete, manifest.Gaps)
	}
	rc, err := newRepairContext(src, size, opts)
	if err != nil {
		return RepairProof{}, err
	}
	if rc.opts.BlockSize != manifest.BlockSize || settingsFingerprint(rc.opts) != manifest.SettingsFingerprint {
		return RepairProof{}, fmt.Errorf("%w: repair settings differ", ErrIncompatibleState)
	}
	if err := rc.verifyFrames(); err != nil {
		return RepairProof{}, err
	}
	if manifest.BlockCount != len(rc.entries) {
		return RepairProof{}, fmt.Errorf("%w: block count differs", ErrIncompatibleState)
	}

	frameBySeq := map[int]RepairFrame{}
	sourceBySeq := map[int][]byte{}
	for _, bundle := range bundles {
		for _, frame := range bundle.Manifest.Frames {
			if existing, ok := frameBySeq[frame.Seq]; ok {
				if existing.Checksum != frame.Checksum {
					return RepairProof{}, fmt.Errorf("%w: block %d", ErrRepairConflict, frame.Seq)
				}
				continue
			}
			frameBySeq[frame.Seq] = frame
			sourceBySeq[frame.Seq] = bundle.Data[frame.Offset : frame.Offset+frame.Length]
		}
	}

	requested := map[int]bool{}
	for _, seq := range manifest.RequestedSeqs() {
		requested[seq] = true
	}
	for seq := range frameBySeq {
		if !requested[seq] {
			return RepairProof{}, fmt.Errorf("%w: candidate covers unrequested block %d", ErrBadArchive, seq)
		}
	}

	proof := RepairProof{BlockCount: len(rc.entries), Requested: manifest.RequestedSeqs()}
	cw := &countingWriter{w: w}
	if err := writeHeader(cw); err != nil {
		return proof, err
	}
	entries := make([]Entry, 0, len(rc.entries))

	// Outside spans are accumulated as contiguous ranges of original
	// bytes; they are the proof that requested-range data alone changed.
	outside := []ByteRange{}
	flushOutside := func(start, end int64) {
		if end <= start {
			return
		}
		if len(outside) > 0 && outside[len(outside)-1].Offset+outside[len(outside)-1].Length == start {
			outside[len(outside)-1].Length += end - start
		} else {
			outside = append(outside, ByteRange{Offset: start, Length: end - start})
		}
	}
	outsideStart := int64(headerSize)

	for seq, entry := range rc.entries {
		var raw []byte
		if requested[seq] {
			candidateFrame, repaired := frameBySeq[seq]
			if !repaired {
				return proof, fmt.Errorf("%w: requested block %d not covered", ErrRepairIncomplete, seq)
			}
			if candidateFrame.Checksum != entry.Checksum {
				return proof, fmt.Errorf("%w: block %d input version mismatch", ErrRepairConflict, seq)
			}
			if candidateFrame.Regenerated {
				if rc.damaged[seq] {
					proof.Regenerated = append(proof.Regenerated, seq)
					proof.RewrittenBlocks++
				} else {
					proof.ReusedInRange = append(proof.ReusedInRange, seq)
					proof.UnchangedBlocks++
				}
			} else {
				if !rc.damaged[seq] {
					proof.ReusedInRange = append(proof.ReusedInRange, seq)
					proof.UnchangedBlocks++
				} else {
					return proof, fmt.Errorf("%w: damaged block %d not regenerated", ErrRepairIncomplete, seq)
				}
			}
			raw = sourceBySeq[seq]
		} else {
			if rc.damaged[seq] {
				return proof, fmt.Errorf("%w: block %d outside request is damaged", ErrRepairIncomplete, seq)
			}
			proof.UnchangedBlocks++
			raw = rc.original[seq]
			flushOutside(outsideStart, entry.DataOffset+entry.CompressedLength)
		}
		outsideStart = entry.DataOffset + entry.CompressedLength

		dataOffset := cw.n
		if _, err := cw.Write(raw); err != nil {
			return proof, err
		}
		entries = append(entries, Entry{
			Seq:              seq,
			OriginalOffset:   entry.OriginalOffset,
			OriginalLength:   entry.OriginalLength,
			DataOffset:       dataOffset,
			CompressedLength: int64(len(raw)),
			Codec:            entry.Codec,
			Checksum:         entry.Checksum,
		})
	}
	if !requested[len(rc.entries)-1] {
		last := rc.entries[len(rc.entries)-1]
		flushOutside(outsideStart, last.DataOffset+last.CompressedLength)
	}
	proof.OutsideRange = outside

	indexOffset := cw.n
	indexChecksum, err := writeIndex(cw, entries)
	if err != nil {
		return proof, err
	}
	if err := writeFooter(cw, entries, indexOffset, indexChecksum); err != nil {
		return proof, err
	}
	return proof, nil
}

var _ = bytes.Equal
var _ = errors.Is

// --- file-level repair API ---

func repairCandidatePaths(path string) (string, string) {
	return path + ".repair", path + ".repair.json"
}

// RepairFile runs a scoped repair task without touching path. The verified
// candidate is written to path+".repair" and its manifest to
// path+".repair.json"; both are kept alongside the original. Restarting
// after cancellation reuses the already confirmed candidate and continues
// from the first unconfirmed frame; blocks produced from different input
// versions are never mixed.
func RepairFile(ctx context.Context, path string, input io.Reader, opts Options, task RepairTask) (RepairPlan, RepairManifest, error) {
	return RepairFileResume(ctx, path, input, opts, task, false)
}

// ResumeRepairFile continues a previously interrupted RepairFile from the
// confirmed frames recorded in path+".repair.json".
func ResumeRepairFile(ctx context.Context, path string, input io.Reader, opts Options, task RepairTask) (RepairPlan, RepairManifest, error) {
	return RepairFileResume(ctx, path, input, opts, task, true)
}

// RepairFileResume implements RepairFile and ResumeRepairFile. When resume
// is set the manifest and candidate must already exist; the input is
// fingerprinted and the confirmed frames are verified against it before
// anything new is encoded.
func RepairFileResume(ctx context.Context, path string, input io.Reader, opts Options, task RepairTask, resume bool) (RepairPlan, RepairManifest, error) {
	if input != nil {
		task.Input = input
	}
	file, err := os.Open(path)
	if err != nil {
		return RepairPlan{}, RepairManifest{}, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return RepairPlan{}, RepairManifest{}, err
	}
	candidatePath, manifestPath := repairCandidatePaths(path)

	var base *RepairManifest
	var baseData []byte
	if resume {
		m, err := LoadManifest(manifestPath)
		if err != nil {
			return RepairPlan{}, RepairManifest{}, err
		}
		baseData, err = os.ReadFile(candidatePath)
		if err != nil {
			return RepairPlan{}, RepairManifest{}, err
		}
		expected := int64(headerSize)
		for _, frame := range m.Frames {
			expected = frame.Offset + frame.Length
		}
		if int64(len(baseData)) != expected {
			return RepairPlan{}, m, fmt.Errorf("%w: candidate length", ErrIncompatibleState)
		}
		base = &m
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".repair-*")
	if err != nil {
		return RepairPlan{}, RepairManifest{}, err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(tmpPath)
		}
	}()

	var saver func(RepairManifest) error
	if !resume {
		saver = func(cp RepairManifest) error {
			return SaveManifest(manifestPath, cp)
		}
	} else {
		// Re-emit confirmed frames into the new candidate and hand the
		// running manifest the already proven prefix.
		if _, err := tmp.Write(baseData); err != nil {
			_ = tmp.Close()
			return RepairPlan{}, *base, err
		}
		saver = func(cp RepairManifest) error {
			return SaveManifest(manifestPath, cp)
		}
	}

	plan, manifest, err := repairWithBase(ctx, tmp, file, info.Size(), opts, task, base, saver)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Cancellation keeps the confirmed prefix: publish the
			// candidate holding exactly the frames the manifest has
			// already recorded, so a later resume continues from them.
			expected := int64(headerSize)
			for _, frame := range manifest.Frames {
				expected = frame.Offset + frame.Length
			}
			if truncErr := tmp.Truncate(expected); truncErr == nil {
				if syncErr := tmp.Sync(); syncErr == nil {
					if closeErr := tmp.Close(); closeErr == nil {
						if renameErr := os.Rename(tmpPath, candidatePath); renameErr == nil {
							cleanup = false
						}
					}
				}
			}
		}
		_ = tmp.Close()
		return plan, manifest, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return plan, manifest, err
	}
	if err := tmp.Close(); err != nil {
		return plan, manifest, err
	}
	if err := os.Rename(tmpPath, candidatePath); err != nil {
		return plan, manifest, err
	}
	cleanup = false
	return plan, manifest, nil
}

// LoadRepairBundle reads a candidate previously written by RepairFile.
func LoadRepairBundle(path string) (RepairBundle, error) {
	candidatePath, manifestPath := repairCandidatePaths(path)
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		return RepairBundle{}, err
	}
	data, err := os.ReadFile(candidatePath)
	if err != nil {
		return RepairBundle{}, err
	}
	return RepairBundle{Manifest: manifest, Data: data}, nil
}

// CommitRepairFile is the only operation that replaces the original. It
// assembles into a temp file next to path, fully verifies the result, then
// atomically renames it over path; on success the candidate files are
// removed. With gaps or conflicts it returns an error and leaves both the
// original and every candidate untouched.
func CommitRepairFile(ctx context.Context, path string, opts Options, bundles ...RepairBundle) (RepairProof, error) {
	_ = ctx
	file, err := os.Open(path)
	if err != nil {
		return RepairProof{}, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return RepairProof{}, err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".commit-*")
	if err != nil {
		_ = file.Close()
		return RepairProof{}, err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		if !committed {
			os.Remove(tmpPath)
		}
	}()

	proof, err := CommitRepair(tmp, file, info.Size(), opts, bundles...)
	if err != nil {
		_ = tmp.Close()
		_ = file.Close()
		return RepairProof{}, err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = file.Close()
		return RepairProof{}, err
	}
	if err := tmp.Close(); err != nil {
		_ = file.Close()
		return RepairProof{}, err
	}
	if err := file.Close(); err != nil {
		return RepairProof{}, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return RepairProof{}, err
	}
	committed = true
	candidatePath, manifestPath := repairCandidatePaths(path)
	_ = os.Remove(candidatePath)
	_ = os.Remove(manifestPath)
	if dirFile, dirErr := os.Open(dir); dirErr == nil {
		defer dirFile.Close()
		_ = dirFile.Sync()
	}
	return proof, nil
}
