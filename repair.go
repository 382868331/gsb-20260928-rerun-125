package datacompression

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
)

// ErrRepairConflict reports that two repair inputs cannot be combined into
// one file: overlapping blocks were regenerated from different input
// versions, or a confirmed block can no longer be reproduced.
var ErrRepairConflict = errors.New("repair inputs conflict")

// BlockStatus classifies one archive block inside a requested repair range.
type BlockStatus uint8

const (
	// BlockReusable blocks verify against the damaged archive and can be
	// copied into the candidate without re-encoding.
	BlockReusable BlockStatus = iota + 1
	// BlockRegenerate blocks are damaged in the archive but the source
	// input covers them, so they must be re-encoded.
	BlockRegenerate
	// BlockMissingInput blocks are damaged and the source input does not
	// cover them; the candidate keeps a gap until the input is available.
	BlockMissingInput
	// BlockConflict blocks verify in the archive but the provided input
	// disagrees with them, so the input belongs to a different version.
	BlockConflict
)

func (s BlockStatus) String() string {
	switch s {
	case BlockReusable:
		return "reusable"
	case BlockRegenerate:
		return "regenerate"
	case BlockMissingInput:
		return "missing-input"
	case BlockConflict:
		return "conflict"
	}
	return "unknown"
}

// RepairBlock pairs an archive block with its repair classification. Digest
// identifies the input version a regenerated block was derived from; blocks
// regenerated from different input versions must never be spliced into one
// candidate.
type RepairBlock struct {
	Entry   Entry       `json:"entry"`
	InRange bool        `json:"in_range"`
	Status  BlockStatus `json:"status"`
	Digest  uint64      `json:"digest,omitempty"`
}

// RepairPlan is the classification of every archive block for one requested
// range. Blocks outside the range are always reused verbatim; the in-range
// subsets are exposed through Reusable, Regenerate, Missing and Conflicts.
type RepairPlan struct {
	RangeStart  int64         `json:"range_start"`
	RangeEnd    int64         `json:"range_end"`
	BlockSize   int           `json:"block_size"`
	InputDigest uint64        `json:"input_digest"`
	Blocks      []RepairBlock `json:"blocks"`
	Truncated   bool          `json:"truncated,omitempty"`
}

func (p *RepairPlan) selectInRange(status BlockStatus) []RepairBlock {
	var out []RepairBlock
	for _, block := range p.Blocks {
		if block.InRange && block.Status == status {
			out = append(out, block)
		}
	}
	return out
}

// Reusable lists in-range blocks that verified against the damaged archive.
func (p *RepairPlan) Reusable() []RepairBlock { return p.selectInRange(BlockReusable) }

// Regenerate lists in-range blocks that must be re-encoded from the input.
func (p *RepairPlan) Regenerate() []RepairBlock { return p.selectInRange(BlockRegenerate) }

// Missing lists in-range blocks that cannot be repaired yet because the
// source input does not cover them.
func (p *RepairPlan) Missing() []RepairBlock { return p.selectInRange(BlockMissingInput) }

// Conflicts lists in-range blocks where the input disagrees with verified
// archive content, meaning the input is a different version of the data.
func (p *RepairPlan) Conflicts() []RepairBlock { return p.selectInRange(BlockConflict) }

func codecMapFor(extra []Codec) map[uint16]Codec {
	codecMap := make(map[uint16]Codec)
	for _, codec := range append(DefaultCodecs(), extra...) {
		if codec != nil {
			codecMap[codec.ID()] = codec
		}
	}
	return codecMap
}

func coversInput(inputSize int64, entry Entry) bool {
	return entry.OriginalOffset >= 0 && entry.OriginalOffset+entry.OriginalLength <= inputSize
}

func digestInput(input io.ReaderAt, size int64) (uint64, error) {
	if input == nil {
		return 0, nil
	}
	h := fnv.New64a()
	buf := make([]byte, 32*1024)
	for offset := int64(0); offset < size; {
		n := int64(len(buf))
		if offset+n > size {
			n = size - offset
		}
		if _, err := input.ReadAt(buf[:n], offset); err != nil {
			return 0, err
		}
		h.Write(buf[:n])
		offset += n
	}
	return h.Sum64(), nil
}

// PlanRepair classifies the blocks of a damaged archive for the requested
// original-data range [start, start+length). Every frame is decoded and
// checksum-verified: verified frames are reusable, damaged frames covered by
// input must be regenerated, and damaged frames beyond the input are
// temporarily missing. When input covers a reusable in-range block it is
// cross-checked against the recorded checksum; a mismatch means the input is
// a different version and the block is reported as a conflict. input may be
// nil when no source data is available.
func PlanRepair(archive io.ReaderAt, archiveSize int64, input io.ReaderAt, inputSize int64, start, length int64, opts Options) (*RepairPlan, error) {
	if archive == nil || start < 0 || length <= 0 {
		return nil, ErrInvalidArgument
	}
	opts = opts.withDefaults()
	codecMap := codecMapFor(opts.Codecs)
	end := start + length

	var entries []Entry
	truncated := false
	if reader, err := NewReader(archive, archiveSize, opts.Codecs); err == nil {
		entries = reader.Entries()
	} else if partial, perr := NewPartialReader(archive, archiveSize, opts.Codecs); perr == nil {
		entries = partial.Entries()
		if !partial.Complete() {
			if input != nil {
				offset := int64(0)
				if len(entries) > 0 {
					last := entries[len(entries)-1]
					offset = last.OriginalOffset + last.OriginalLength
				}
				for seq := len(entries); offset < inputSize; seq++ {
					n := int64(opts.BlockSize)
					if offset+n > inputSize {
						n = inputSize - offset
					}
					entries = append(entries, Entry{Seq: seq, OriginalOffset: offset, OriginalLength: n})
					offset += n
				}
			} else {
				truncated = true
			}
		}
	} else {
		return nil, perr
	}

	digest, err := digestInput(input, inputSize)
	if err != nil {
		return nil, err
	}
	plan := &RepairPlan{
		RangeStart:  start,
		RangeEnd:    end,
		BlockSize:   opts.BlockSize,
		InputDigest: digest,
		Truncated:   truncated,
	}
	for i := range entries {
		entry := entries[i]
		inRange := entry.OriginalOffset < end && entry.OriginalOffset+entry.OriginalLength > start
		block := RepairBlock{Entry: entry, InRange: inRange}
		_, frameErr := readBlockAt(archive, codecMap, entry)
		switch {
		case frameErr == nil:
			block.Status = BlockReusable
			if inRange && input != nil && coversInput(inputSize, entry) {
				data, err := readInputSlice(input, entry)
				if err != nil {
					return nil, err
				}
				if crc32Checksum(data) != entry.Checksum {
					block.Status = BlockConflict
				}
			}
		case inRange && input != nil && coversInput(inputSize, entry):
			block.Status = BlockRegenerate
			block.Digest = digest
		default:
			block.Status = BlockMissingInput
		}
		plan.Blocks = append(plan.Blocks, block)
	}
	return plan, nil
}

func readInputSlice(input io.ReaderAt, entry Entry) ([]byte, error) {
	data := make([]byte, entry.OriginalLength)
	if _, err := input.ReadAt(data, entry.OriginalOffset); err != nil {
		return nil, err
	}
	return data, nil
}

// RepairGap describes a block the candidate could not fill.
type RepairGap struct {
	Seq            int    `json:"seq"`
	OriginalOffset int64  `json:"original_offset"`
	OriginalLength int64  `json:"original_length"`
	Reason         string `json:"reason"`
}

// RepairState is the durable progress of a range repair. Confirmed blocks
// form a verified prefix of the candidate; a cancelled or reopened repair
// resumes exactly after them.
type RepairState struct {
	Version      int           `json:"version"`
	RangeStart   int64         `json:"range_start"`
	RangeEnd     int64         `json:"range_end"`
	InputDigest  uint64        `json:"input_digest"`
	Plan         *RepairPlan   `json:"plan"`
	Confirmed    []RepairBlock `json:"confirmed"`
	OutputOffset int64         `json:"output_offset"`
	Gaps         []RepairGap   `json:"gaps,omitempty"`
	Done         bool          `json:"done"`
}

// RepairReport summarizes one repair run. ReplaceOriginal is true only when
// the candidate is a complete archive and every block outside the requested
// range is byte-identical to the original file; otherwise the candidate is
// delivered with Gaps and the original must be kept.
type RepairReport struct {
	Complete             bool
	ReplaceOriginal      bool
	OutsideRangeVerified bool
	Reused               int
	Regenerated          int
	Gaps                 []RepairGap
	State                RepairState
}

// RunRepair executes plan into w, writing a candidate archive. Reusable
// frames are copied verbatim from the damaged archive, damaged in-range
// blocks are re-encoded from input, and the run stops at the first block
// that cannot be produced, recording a gap. state carries the confirmed
// prefix across cancellation: pass the previous state and a writer
// positioned at state.OutputOffset to resume. hook is invoked after every
// confirmed block so callers can persist the state.
func RunRepair(ctx context.Context, w io.Writer, archive io.ReaderAt, input io.ReaderAt, inputSize int64, plan *RepairPlan, opts Options, state *RepairState, hook func(RepairState) error) (*RepairReport, error) {
	if w == nil || archive == nil || plan == nil {
		return nil, ErrInvalidArgument
	}
	opts = opts.withDefaults()
	codecMap := codecMapFor(opts.Codecs)
	if state == nil {
		state = &RepairState{
			Version:     formatVersion,
			RangeStart:  plan.RangeStart,
			RangeEnd:    plan.RangeEnd,
			InputDigest: plan.InputDigest,
			Plan:        plan,
		}
	}
	report := &RepairReport{}
	confirmed := len(state.Confirmed)
	if confirmed > len(plan.Blocks) {
		return report, fmt.Errorf("%w: confirmed prefix longer than plan", ErrRepairConflict)
	}
	for i := 0; i < confirmed; i++ {
		prev := state.Confirmed[i]
		if prev.Entry.Seq != plan.Blocks[i].Entry.Seq {
			return report, fmt.Errorf("%w: confirmed block %d no longer matches plan", ErrRepairConflict, i)
		}
		if prev.Status == BlockRegenerate && prev.Digest != plan.InputDigest {
			return report, fmt.Errorf("%w: block %d was generated from a different input version", ErrRepairConflict, i)
		}
	}
	if confirmed > 0 {
		last := state.Confirmed[confirmed-1].Entry
		if state.OutputOffset != last.DataOffset+last.CompressedLength {
			return report, fmt.Errorf("%w: state output offset", ErrRepairConflict)
		}
	}
	state.Gaps = nil

	cw := &countingWriter{w: w, n: state.OutputOffset}
	if state.OutputOffset == 0 {
		if confirmed != 0 {
			return report, fmt.Errorf("%w: confirmed blocks without output", ErrRepairConflict)
		}
		if err := writeHeader(cw); err != nil {
			return report, err
		}
	}
	entries := make([]Entry, 0, len(plan.Blocks))
	for _, block := range state.Confirmed {
		entries = append(entries, block.Entry)
	}
	save := func() error {
		state.OutputOffset = cw.n
		if hook != nil {
			return hook(*state)
		}
		return nil
	}
	fail := func(err error) (*RepairReport, error) {
		_ = save()
		report.State = *state
		report.Gaps = state.Gaps
		return report, err
	}

	for i := confirmed; i < len(plan.Blocks); i++ {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		block := plan.Blocks[i]
		dataOffset := cw.n
		switch block.Status {
		case BlockReusable:
			if _, err := readBlockAt(archive, codecMap, block.Entry); err != nil {
				return fail(fmt.Errorf("re-verify block %d: %w", i, err))
			}
			frame := make([]byte, block.Entry.CompressedLength)
			if _, err := archive.ReadAt(frame, block.Entry.DataOffset); err != nil {
				return fail(err)
			}
			if err := writeFull(cw, frame); err != nil {
				return fail(err)
			}
			block.Entry.DataOffset = dataOffset
			if block.InRange {
				report.Reused++
			}
		case BlockRegenerate:
			data, err := readInputSlice(input, block.Entry)
			if err != nil {
				return fail(err)
			}
			result := encodeBlock(Block{Seq: i, OriginalOffset: block.Entry.OriginalOffset, Data: data}, opts)
			if result.err != nil {
				return fail(result.err)
			}
			if err := writeFrame(cw, result); err != nil {
				return fail(err)
			}
			block.Entry = Entry{
				Seq:              i,
				OriginalOffset:   block.Entry.OriginalOffset,
				OriginalLength:   int64(len(data)),
				DataOffset:       dataOffset,
				CompressedLength: int64(frameHeaderSize + len(result.payload)),
				Codec:            result.codec,
				Checksum:         result.checksum,
			}
			block.Digest = plan.InputDigest
			if block.InRange {
				report.Regenerated++
			}
		default:
			reason := "missing input"
			switch {
			case block.Status == BlockConflict:
				reason = "input conflicts with verified archive content"
			case !block.InRange:
				reason = "damaged outside requested range"
			}
			state.Gaps = append(state.Gaps, RepairGap{
				Seq:            i,
				OriginalOffset: block.Entry.OriginalOffset,
				OriginalLength: block.Entry.OriginalLength,
				Reason:         reason,
			})
			report.State = *state
			report.Gaps = state.Gaps
			return fail(nil)
		}
		entries = append(entries, block.Entry)
		state.Confirmed = append(state.Confirmed, block)
		if err := save(); err != nil {
			report.State = *state
			return report, err
		}
	}
	if plan.Truncated {
		state.Gaps = append(state.Gaps, RepairGap{
			Seq:    len(plan.Blocks),
			Reason: "archive truncated and input unavailable",
		})
		return fail(nil)
	}

	indexOffset := cw.n
	indexChecksum, err := writeIndex(cw, entries)
	if err != nil {
		return fail(err)
	}
	if err := writeFooter(cw, entries, indexOffset, indexChecksum); err != nil {
		return fail(err)
	}
	state.OutputOffset = cw.n
	state.Done = true
	if err := save(); err != nil {
		report.State = *state
		return report, err
	}
	report.Complete = true
	report.State = *state
	return report, nil
}

// MergeRepairPlans combines two repair tasks over adjacent or overlapping
// ranges of the same archive. Overlapping blocks collapse to a single
// verified result: reusable (archive-verified) content wins, and blocks
// regenerated from different input versions are rejected with
// ErrRepairConflict because they must never be spliced into one file.
func MergeRepairPlans(a, b *RepairPlan) (*RepairPlan, error) {
	if a == nil || b == nil {
		return nil, ErrInvalidArgument
	}
	if a.BlockSize != b.BlockSize {
		return nil, fmt.Errorf("%w: block size differs", ErrRepairConflict)
	}
	if a.RangeEnd < b.RangeStart || b.RangeEnd < a.RangeStart {
		return nil, fmt.Errorf("%w: ranges are disjoint", ErrRepairConflict)
	}
	if len(a.Blocks) != len(b.Blocks) {
		return nil, fmt.Errorf("%w: archive block list changed", ErrRepairConflict)
	}
	merged := &RepairPlan{
		RangeStart:  minInt64(a.RangeStart, b.RangeStart),
		RangeEnd:    maxInt64(a.RangeEnd, b.RangeEnd),
		BlockSize:   a.BlockSize,
		InputDigest: a.InputDigest,
		Truncated:   a.Truncated && b.Truncated,
	}
	for i := range a.Blocks {
		block, err := mergeRepairBlock(a.Blocks[i], b.Blocks[i])
		if err != nil {
			return nil, err
		}
		block.InRange = a.Blocks[i].InRange || b.Blocks[i].InRange
		merged.Blocks = append(merged.Blocks, block)
	}
	return merged, nil
}

func mergeRepairBlock(x, y RepairBlock) (RepairBlock, error) {
	if x.Entry.Seq != y.Entry.Seq {
		return x, fmt.Errorf("%w: block order differs", ErrRepairConflict)
	}
	if x.Status == y.Status {
		switch x.Status {
		case BlockReusable:
			if x.Entry.Checksum != y.Entry.Checksum {
				return x, fmt.Errorf("%w: block %d archive content changed", ErrRepairConflict, x.Entry.Seq)
			}
		case BlockRegenerate:
			if x.Digest != y.Digest {
				return x, fmt.Errorf("%w: block %d generated from different input versions", ErrRepairConflict, x.Entry.Seq)
			}
		}
		return x, nil
	}
	if x.Status == BlockReusable {
		return x, nil
	}
	if y.Status == BlockReusable {
		return y, nil
	}
	if x.Status == BlockRegenerate && y.Status == BlockMissingInput {
		return x, nil
	}
	if y.Status == BlockRegenerate && x.Status == BlockMissingInput {
		return y, nil
	}
	return x, fmt.Errorf("%w: block %d cannot be reconciled", ErrRepairConflict, x.Entry.Seq)
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func repairCandidatePath(path string) string { return path + ".repair" }
func repairStatePath(path string) string     { return path + ".repairstate" }

func saveRepairState(path string, state RepairState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func loadRepairState(path string) (*RepairState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state RepairState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRepairConflict, err)
	}
	if state.Plan == nil {
		return nil, fmt.Errorf("%w: state has no plan", ErrRepairConflict)
	}
	return &state, nil
}

// RepairFile repairs the requested range of the damaged archive at path.
// The original file is never modified: the candidate is written to
// path+".repair" and durable progress to path+".repairstate", so a
// cancelled run resumes from its confirmed prefix on the next call and
// later calls with adjacent or overlapping ranges extend the same
// candidate. When the resumed input is a different version, confirmed
// regenerated blocks are re-derived from the new input; if one of them is
// no longer covered, ErrRepairConflict is returned and nothing is written.
//
// The returned report decides delivery: only when Complete and
// OutsideRangeVerified are both true (ReplaceOriginal) may the candidate
// replace the original; otherwise the candidate carries gaps and the
// original must be kept.
func RepairFile(ctx context.Context, path string, input io.ReaderAt, inputSize int64, start, length int64, opts Options) (*RepairReport, error) {
	if path == "" {
		return nil, ErrInvalidArgument
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	archive, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer archive.Close()

	plan, err := PlanRepair(archive, info.Size(), input, inputSize, start, length, opts)
	if err != nil {
		return nil, err
	}
	statePath := repairStatePath(path)
	state, err := loadRepairState(statePath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if state != nil {
		if state.Done {
			return nil, fmt.Errorf("%w: repair already finalized", ErrRepairConflict)
		}
		if state.InputDigest != plan.InputDigest {
			drop := len(state.Confirmed)
			for i, block := range state.Confirmed {
				if block.Status == BlockRegenerate && block.Digest != plan.InputDigest {
					drop = i
					break
				}
			}
			for _, block := range state.Confirmed[drop:] {
				if block.Status == BlockRegenerate && !coversInput(inputSize, block.Entry) {
					return nil, fmt.Errorf("%w: block %d was generated from a different input version and is not covered by the current input", ErrRepairConflict, block.Entry.Seq)
				}
			}
			state.Confirmed = state.Confirmed[:drop]
			if drop == 0 {
				state.OutputOffset = 0
			} else {
				last := state.Confirmed[drop-1].Entry
				state.OutputOffset = last.DataOffset + last.CompressedLength
			}
		}
		plan.RangeStart = minInt64(plan.RangeStart, state.RangeStart)
		plan.RangeEnd = maxInt64(plan.RangeEnd, state.RangeEnd)
		for i := range plan.Blocks {
			if plan.Blocks[i].Entry.OriginalOffset < plan.RangeEnd &&
				plan.Blocks[i].Entry.OriginalOffset+plan.Blocks[i].Entry.OriginalLength > plan.RangeStart {
				plan.Blocks[i].InRange = true
			}
		}
		state.Plan = plan
		state.InputDigest = plan.InputDigest
		state.RangeStart = plan.RangeStart
		state.RangeEnd = plan.RangeEnd
	}

	candidate := repairCandidatePath(path)
	file, err := os.OpenFile(candidate, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	outputOffset := int64(0)
	if state != nil {
		outputOffset = state.OutputOffset
	}
	if err := file.Truncate(outputOffset); err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(outputOffset, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, err
	}

	hook := func(s RepairState) error { return saveRepairState(statePath, s) }
	report, runErr := RunRepair(ctx, file, archive, input, inputSize, plan, opts, state, hook)
	if runErr != nil {
		_ = file.Close()
		return report, runErr
	}
	if !report.Complete {
		_ = file.Sync()
		_ = file.Close()
		return report, nil
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return report, err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return report, err
	}
	candidateInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return report, err
	}
	reader, err := NewReader(file, candidateInfo.Size(), opts.Codecs)
	if err != nil {
		_ = file.Close()
		return report, fmt.Errorf("candidate does not decode: %w", err)
	}
	for i := range reader.Entries() {
		if _, err := reader.ReadBlock(i); err != nil {
			_ = file.Close()
			return report, fmt.Errorf("candidate block %d does not decode: %w", i, err)
		}
	}
	verified, err := verifyOutsideRange(archive, file, plan, reader.Entries())
	if err != nil {
		_ = file.Close()
		return report, err
	}
	report.OutsideRangeVerified = verified
	report.ReplaceOriginal = verified
	if err := file.Close(); err != nil {
		return report, err
	}
	if err := os.Remove(statePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return report, err
	}
	return report, nil
}

// verifyOutsideRange proves the repair did not rewrite anything outside the
// requested range: every out-of-range frame in the candidate must be
// byte-identical to the frame in the original damaged archive.
func verifyOutsideRange(archive, candidate io.ReaderAt, plan *RepairPlan, entries []Entry) (bool, error) {
	if len(entries) != len(plan.Blocks) {
		return false, nil
	}
	for i, block := range plan.Blocks {
		if block.InRange {
			continue
		}
		original := make([]byte, block.Entry.CompressedLength)
		if _, err := archive.ReadAt(original, block.Entry.DataOffset); err != nil {
			return false, err
		}
		repaired := make([]byte, entries[i].CompressedLength)
		if _, err := candidate.ReadAt(repaired, entries[i].DataOffset); err != nil {
			return false, err
		}
		if !bytes.Equal(original, repaired) {
			return false, nil
		}
	}
	return true, nil
}
