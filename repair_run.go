package datacompression

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
)

// ByteRange identifies a byte span copied unchanged from the original
// archive during a commit.
type ByteRange struct {
	Offset int64
	Length int64
}

// RepairProof is produced by a successful commit. It shows which blocks
// were regenerated, which were reused inside the request and the exact
// original byte spans copied without modification outside it, proving the
// commit did not rewrite anything beyond the requested range.
type RepairProof struct {
	BlockCount      int
	Requested       []int
	Regenerated     []int
	ReusedInRange   []int
	OutsideRange    []ByteRange
	UnchangedBlocks int
	RewrittenBlocks int
}

// RepairTask is one scoped repair request over a damaged archive.
type RepairTask struct {
	// Ranges limits the repair to the given block sequence ranges.
	Ranges []RepairRange
	// Input yields the original data from the very beginning. Only the
	// blocks inside Ranges are consumed; blocks outside the request are
	// never reprocessed.
	Input io.Reader
	// Concurrency bounds parallel frame regeneration.
	Concurrency int
}

func (t RepairTask) withDefaults() RepairTask {
	if t.Concurrency <= 0 {
		t.Concurrency = 4
	}
	return t
}

type repairContext struct {
	opts     Options
	entries  []Entry
	codecs   map[uint16]Codec
	src      io.ReaderAt
	size     int64
	damaged  map[int]bool
	original map[int][]byte
}

func newRepairContext(src io.ReaderAt, size int64, opts Options) (*repairContext, error) {
	if src == nil {
		return nil, ErrInvalidArgument
	}
	opts = opts.withDefaults()
	reader, err := NewReader(src, size, opts.Codecs)
	if err != nil {
		return nil, err
	}
	codecMap := make(map[uint16]Codec)
	for _, codec := range append(DefaultCodecs(), opts.Codecs...) {
		if codec != nil {
			codecMap[codec.ID()] = codec
		}
	}
	return &repairContext{
		opts:     opts,
		entries:  reader.Entries(),
		codecs:   codecMap,
		src:      src,
		size:     size,
		damaged:  map[int]bool{},
		original: map[int][]byte{},
	}, nil
}

// verifyFrames reads every indexed frame once. Intact frame bytes are kept
// for byte-for-byte reuse; damaged sequences are recorded for planning.
func (rc *repairContext) verifyFrames() error {
	for _, entry := range rc.entries {
		frame := make([]byte, entry.CompressedLength)
		if _, err := rc.src.ReadAt(frame, entry.DataOffset); err != nil {
			return err
		}
		if _, err := readBlockAt(rc.src, rc.codecs, entry); err != nil {
			rc.damaged[entry.Seq] = true
			continue
		}
		rc.original[entry.Seq] = frame
	}
	return nil
}

// PlanRepair classifies the requested blocks without writing anything.
// Damaged blocks whose input CRC matches the index can be regenerated;
// intact blocks are reused; blocks the input does not cover are missing;
// input whose CRC disagrees is a version conflict and is rejected.
func PlanRepair(src io.ReaderAt, size int64, opts Options, task RepairTask) (RepairPlan, error) {
	rc, err := newRepairContext(src, size, opts)
	if err != nil {
		return RepairPlan{}, err
	}
	if err := rc.verifyFrames(); err != nil {
		return RepairPlan{}, err
	}
	plan, _, err := rc.plan(task)
	return plan, err
}

func (rc *repairContext) plan(task RepairTask) (RepairPlan, map[int][]byte, error) {
	if task.Input == nil {
		return RepairPlan{}, nil, fmt.Errorf("%w: input required", ErrInvalidArgument)
	}
	task = task.withDefaults()
	seqs := rangeSeqs(task.Ranges, len(rc.entries))
	requested := map[int]bool{}
	for _, seq := range seqs {
		requested[seq] = true
	}
	provided := map[int][]byte{}
	fingerprint, err := readRequestedBlocks(task.Input, rc.entries, requested, provided)
	if err != nil {
		return RepairPlan{}, nil, err
	}

	plan := RepairPlan{
		BlockCount:       len(rc.entries),
		Ranges:           mergeRanges(task.Ranges),
		InputFingerprint: fingerprint,
	}
	for _, seq := range seqs {
		status := RepairReuse
		if rc.damaged[seq] {
			data, ok := provided[seq]
			switch {
			case !ok:
				status = RepairMissingInput
			case crc32Checksum(data) != rc.entries[seq].Checksum:
				status = RepairInputConflict
			default:
				status = RepairRegenerate
			}
		}
		plan.Items = append(plan.Items, RepairItem{Seq: seq, Status: status})
	}
	return plan, provided, nil
}

type preparedFrame struct {
	seq         int
	regenerated bool
	codec       uint16
	data        []byte
	payload     []byte
	checksum    uint32
	err         error
}

// Repair runs one scoped repair task into candidate. The candidate receives
// the archive header followed by verified frames covering the requested
// range, in sequence order. The returned manifest records gaps when input
// was missing or from a different input version; save is called after
// every confirmed frame, so cancellation resumes from confirmed frames.
// The original archive is never written.
func Repair(ctx context.Context, candidate io.Writer, src io.ReaderAt, size int64, opts Options, task RepairTask, save func(RepairManifest) error) (RepairPlan, RepairManifest, error) {
	return repairWithBase(ctx, candidate, src, size, opts, task, nil, save)
}

func repairWithBase(ctx context.Context, candidate io.Writer, src io.ReaderAt, size int64, opts Options, task RepairTask, base *RepairManifest, save func(RepairManifest) error) (RepairPlan, RepairManifest, error) {
	rc, err := newRepairContext(src, size, opts)
	if err != nil {
		return RepairPlan{}, RepairManifest{}, err
	}
	if err := rc.verifyFrames(); err != nil {
		return RepairPlan{}, RepairManifest{}, err
	}
	plan, provided, err := rc.plan(task)
	if err != nil {
		return plan, RepairManifest{}, err
	}
	task = task.withDefaults()

	manifest := RepairManifest{
		Version:             formatVersion,
		BlockCount:          len(rc.entries),
		BlockSize:           rc.opts.BlockSize,
		SettingsFingerprint: settingsFingerprint(rc.opts),
		InputFingerprint:    plan.InputFingerprint,
		Ranges:              plan.Ranges,
	}
	seqs := rangeSeqs(task.Ranges, len(rc.entries))
	gaps := map[int]bool{}
	toGenerate := map[int][]byte{}
	for _, item := range plan.Items {
		switch item.Status {
		case RepairRegenerate:
			toGenerate[item.Seq] = provided[item.Seq]
		case RepairMissingInput, RepairInputConflict:
			gaps[item.Seq] = true
		}
	}

	confirmed := map[int]bool{}
	if base != nil {
		if base.Version != formatVersion || base.BlockCount != len(rc.entries) ||
			base.BlockSize != rc.opts.BlockSize || base.SettingsFingerprint != settingsFingerprint(rc.opts) {
			return plan, RepairManifest{}, fmt.Errorf("%w: resume settings differ", ErrIncompatibleState)
		}
		if base.InputFingerprint != plan.InputFingerprint {
			return plan, RepairManifest{}, fmt.Errorf("%w: input version changed since the confirmed prefix", ErrIncompatibleState)
		}
		requested := map[int]bool{}
		for _, seq := range seqs {
			requested[seq] = true
		}
		for _, frame := range base.Frames {
			if !requested[frame.Seq] {
				continue
			}
			if frame.Checksum != rc.entries[frame.Seq].Checksum {
				return plan, RepairManifest{}, fmt.Errorf("%w: confirmed block %d input mismatch", ErrIncompatibleState, frame.Seq)
			}
			confirmed[frame.Seq] = true
			delete(gaps, frame.Seq)
		}
	}

	if base == nil {
		if err := writeHeader(candidate); err != nil {
			return plan, manifest, err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	results := make(chan preparedFrame, task.Concurrency)
	var workers sync.WaitGroup
	for worker := 0; worker < task.Concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for seq := range jobs {
				entry := rc.entries[seq]
				if data, ok := toGenerate[seq]; ok {
					encoded := encodeBlock(Block{Seq: seq, OriginalOffset: entry.OriginalOffset, Data: data}, rc.opts)
					select {
					case <-ctx.Done():
						return
					case results <- preparedFrame{
						seq:         seq,
						regenerated: true,
						codec:       encoded.codec,
						data:        data,
						payload:     encoded.payload,
						checksum:    encoded.checksum,
						err:         encoded.err,
					}:
					}
					continue
				}
				frame := rc.original[seq]
				payload := append([]byte(nil), frame[frameHeaderSize:]...)
				select {
				case <-ctx.Done():
					return
				case results <- preparedFrame{
					seq:         seq,
					regenerated: false,
					codec:       entry.Codec,
					payload:     payload,
					checksum:    entry.Checksum,
				}:
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		skip := map[int]bool{}
		for seq := range confirmed {
			skip[seq] = true
		}
		for seq := range gaps {
			skip[seq] = true
		}
		for _, seq := range seqs {
			if skip[seq] {
				continue
			}
			select {
			case <-ctx.Done():
				return
			case jobs <- seq:
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	pending := map[int]preparedFrame{}
	next := 0
	offset := int64(headerSize)
	if base != nil {
		manifest.Ranges = mergeRanges(append(append([]RepairRange{}, manifest.Ranges...), base.Ranges...))
		manifest.Frames = append(manifest.Frames, base.Frames...)
		for _, frame := range base.Frames {
			offset = frame.Offset + frame.Length
		}
	}
publish:
	for {
		for next < len(seqs) && confirmed[seqs[next]] {
			next++
		}
		if next < len(seqs) {
			if frame, ok := pending[seqs[next]]; ok {
				delete(pending, seqs[next])
				if frame.err != nil {
					cancel()
					return plan, manifest, frame.err
				}
				buf := &bytes.Buffer{}
				if frame.regenerated {
					block := resultBlock{
						seq:      frame.seq,
						offset:   rc.entries[frame.seq].OriginalOffset,
						data:     frame.data,
						codec:    frame.codec,
						payload:  frame.payload,
						checksum: frame.checksum,
					}
					if err := writeFrame(buf, block); err != nil {
						cancel()
						return plan, manifest, err
					}
				} else {
					// Reuse the original frame bytes verbatim.
					original := rc.original[frame.seq]
					if len(original) == 0 || binary.LittleEndian.Uint32(original[:4]) != frameMagic {
						cancel()
						return plan, manifest, ErrBadArchive
					}
					buf.Write(original)
				}
				if _, err := candidate.Write(buf.Bytes()); err != nil {
					cancel()
					return plan, manifest, err
				}
				manifest.Frames = append(manifest.Frames, RepairFrame{
					Seq:         frame.seq,
					Offset:      offset,
					Length:      int64(buf.Len()),
					Codec:       frame.codec,
					Checksum:    frame.checksum,
					Regenerated: frame.regenerated,
				})
				offset += int64(buf.Len())
				delete(gaps, frame.seq)
				manifest.Gaps = intSetSorted(gaps)
				manifest.normalize()
				if save != nil {
					if err := save(manifest); err != nil {
						cancel()
						return plan, manifest, err
					}
				}
				next++
				continue
			}
		} else {
			break
		}
		select {
		case <-ctx.Done():
			break publish
		case frame, ok := <-results:
			if !ok {
				break publish
			}
			pending[frame.seq] = frame
		}
	}
	if err := ctx.Err(); err != nil {
		manifest.Gaps = intSetSorted(gaps)
		manifest.normalize()
		if save != nil {
			// Persist the confirmed prefix so a later resume continues
			// from it instead of restarting.
			_ = save(manifest)
		}
		return plan, manifest, err
	}
	manifest.Gaps = intSetSorted(gaps)
	manifest.normalize()
	if save != nil {
		if err := save(manifest); err != nil {
			return plan, manifest, err
		}
	}
	return plan, manifest, nil
}

func intSetSorted(set map[int]bool) []int {
	out := make([]int, 0, len(set))
	for seq, ok := range set {
		if ok {
			out = append(out, seq)
		}
	}
	sort.Ints(out)
	return out
}

// MergeRepairManifests merges candidates covering adjacent or overlapping
// ranges. Overlapping content must agree down to the recorded CRC; a
// mismatch means the candidates came from different input versions and
// ErrRepairConflict is returned. Each repaired block is adopted once.
func MergeRepairManifests(manifests ...RepairManifest) (RepairManifest, error) {
	if len(manifests) == 0 {
		return RepairManifest{}, ErrInvalidArgument
	}
	out := manifests[0]
	out.normalize()
	for _, other := range manifests[1:] {
		other.normalize()
		if other.BlockCount != out.BlockCount || other.Version != out.Version {
			return out, fmt.Errorf("%w: archive identity differs", ErrRepairConflict)
		}
		if other.SettingsFingerprint != out.SettingsFingerprint {
			return out, fmt.Errorf("%w: encoding settings differ", ErrRepairConflict)
		}
		bySeq := map[int]RepairFrame{}
		for _, frame := range out.Frames {
			bySeq[frame.Seq] = frame
		}
		for _, frame := range other.Frames {
			if existing, ok := bySeq[frame.Seq]; ok {
				if existing.Checksum != frame.Checksum {
					return out, fmt.Errorf("%w: block %d content differs", ErrRepairConflict, frame.Seq)
				}
				continue
			}
			bySeq[frame.Seq] = frame
			out.Frames = append(out.Frames, frame)
		}
		out.Ranges = mergeRanges(append(append([]RepairRange{}, out.Ranges...), other.Ranges...))
		gapSet := map[int]bool{}
		for _, seq := range append(append([]int{}, out.Gaps...), other.Gaps...) {
			if _, repaired := bySeq[seq]; !repaired {
				gapSet[seq] = true
			}
		}
		out.Gaps = intSetSorted(gapSet)
		out.normalize()
	}
	return out, nil
}
