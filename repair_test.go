package datacompression

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func compressToBytes(t *testing.T, input []byte, opts Options) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Compress(context.Background(), &buf, bytes.NewReader(input), opts); err != nil {
		t.Fatalf("Compress() error = %v", err)
	}
	return buf.Bytes()
}

// corruptBlockPayload flips the last payload byte of one frame, leaving the
// frame header, index and footer intact.
func corruptBlockPayload(t *testing.T, archive []byte, seq int) {
	t.Helper()
	reader, err := NewReaderFromBytes(archive, nil)
	if err != nil {
		t.Fatalf("NewReaderFromBytes() error = %v", err)
	}
	entry := reader.Entries()[seq]
	archive[entry.DataOffset+entry.CompressedLength-1] ^= 0xFF
}

func blockSeqs(blocks []RepairBlock) []int {
	seqs := make([]int, 0, len(blocks))
	for _, block := range blocks {
		seqs = append(seqs, block.Entry.Seq)
	}
	return seqs
}

func equalSeqs(got []int, want ...int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestRepairPlanClassifiesTwoCorruptions(t *testing.T) {
	blockSize := 64
	input := testInput(8, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}
	archive := compressToBytes(t, input, opts)
	corruptBlockPayload(t, archive, 2)
	corruptBlockPayload(t, archive, 5)

	plan, err := PlanRepair(bytes.NewReader(archive), int64(len(archive)),
		bytes.NewReader(input), int64(len(input)), 0, int64(len(input)), opts)
	if err != nil {
		t.Fatalf("PlanRepair() error = %v", err)
	}
	if got := blockSeqs(plan.Reusable()); !equalSeqs(got, 0, 1, 3, 4, 6, 7) {
		t.Fatalf("reusable = %v, want [0 1 3 4 6 7]", got)
	}
	if got := blockSeqs(plan.Regenerate()); !equalSeqs(got, 2, 5) {
		t.Fatalf("regenerate = %v, want [2 5]", got)
	}
	if got := plan.Missing(); len(got) != 0 {
		t.Fatalf("missing = %v, want none", blockSeqs(got))
	}

	// A short input covers block 2 but not block 5.
	short := input[:4*blockSize]
	plan, err = PlanRepair(bytes.NewReader(archive), int64(len(archive)),
		bytes.NewReader(short), int64(len(short)), 0, int64(len(input)), opts)
	if err != nil {
		t.Fatalf("PlanRepair() error = %v", err)
	}
	if got := blockSeqs(plan.Regenerate()); !equalSeqs(got, 2) {
		t.Fatalf("regenerate = %v, want [2]", got)
	}
	if got := blockSeqs(plan.Missing()); !equalSeqs(got, 5) {
		t.Fatalf("missing = %v, want [5]", got)
	}
}

func TestRepairFileCompleteAndOutsideRangeUntouched(t *testing.T) {
	blockSize := 64
	input := testInput(8, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}
	archive := compressToBytes(t, input, opts)
	corruptBlockPayload(t, archive, 2)
	corruptBlockPayload(t, archive, 5)

	dir := t.TempDir()
	path := filepath.Join(dir, "data.gcbk")
	if err := os.WriteFile(path, archive, 0o644); err != nil {
		t.Fatal(err)
	}

	// Repair only blocks 1..6; blocks 0 and 7 stay outside the range.
	report, err := RepairFile(context.Background(), path,
		bytes.NewReader(input), int64(len(input)), int64(blockSize), int64(6*blockSize), opts)
	if err != nil {
		t.Fatalf("RepairFile() error = %v", err)
	}
	if !report.Complete || !report.ReplaceOriginal {
		t.Fatalf("Complete=%v ReplaceOriginal=%v, want both true", report.Complete, report.ReplaceOriginal)
	}
	if !report.OutsideRangeVerified {
		t.Fatal("outside-range frames were not byte-identical to the original")
	}
	if report.Reused != 4 || report.Regenerated != 2 {
		t.Fatalf("Reused=%d Regenerated=%d, want 4 and 2", report.Reused, report.Regenerated)
	}

	// The original file is untouched and the candidate decodes fully.
	current, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(current, archive) {
		t.Fatal("original archive was modified")
	}
	candidate, err := os.ReadFile(path + ".repair")
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeAll(t, candidate, nil); !bytes.Equal(got, input) {
		t.Fatal("candidate does not decode to the original input")
	}
	if _, err := os.Stat(path + ".repairstate"); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("repair state should be removed once the candidate is complete")
	}
}

func TestRepairResumeAfterCancel(t *testing.T) {
	blockSize := 64
	input := testInput(8, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}
	archive := compressToBytes(t, input, opts)
	corruptBlockPayload(t, archive, 3)

	plan, err := PlanRepair(bytes.NewReader(archive), int64(len(archive)),
		bytes.NewReader(input), int64(len(input)), 0, int64(len(input)), opts)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var first bytes.Buffer
	var state *RepairState
	hook := func(s RepairState) error {
		cp := s
		state = &cp
		if len(s.Confirmed) >= 3 {
			cancel()
		}
		return nil
	}
	_, err = RunRepair(ctx, &first, bytes.NewReader(archive), bytes.NewReader(input), int64(len(input)), plan, opts, nil, hook)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunRepair() error = %v, want context.Canceled", err)
	}
	if state == nil || len(state.Confirmed) != 3 {
		t.Fatalf("confirmed = %d, want 3", len(state.Confirmed))
	}
	interrupted := append([]byte(nil), first.Bytes()...)

	// Reopen: the candidate keeps its confirmed prefix byte for byte.
	var resumed bytes.Buffer
	resumed.Write(interrupted)
	report, err := RunRepair(context.Background(), &resumed, bytes.NewReader(archive),
		bytes.NewReader(input), int64(len(input)), plan, opts, state, nil)
	if err != nil {
		t.Fatalf("resume RunRepair() error = %v", err)
	}
	if !report.Complete {
		t.Fatalf("resume incomplete, gaps = %+v", report.Gaps)
	}
	if !bytes.Equal(resumed.Bytes()[:len(interrupted)], interrupted) {
		t.Fatal("confirmed prefix was rewritten during resume")
	}
	if got := decodeAll(t, resumed.Bytes(), nil); !bytes.Equal(got, input) {
		t.Fatal("resumed candidate does not decode to the original input")
	}
}

func TestRepairInputPartialChange(t *testing.T) {
	blockSize := 64
	input := testInput(8, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}
	archive := compressToBytes(t, input, opts)
	corruptBlockPayload(t, archive, 2)

	// The input changed inside block 4, which the archive still verifies.
	changed := append([]byte(nil), input...)
	changed[4*blockSize] = 'Z'
	plan, err := PlanRepair(bytes.NewReader(archive), int64(len(archive)),
		bytes.NewReader(changed), int64(len(changed)), 0, int64(len(input)), opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := blockSeqs(plan.Conflicts()); !equalSeqs(got, 4) {
		t.Fatalf("conflicts = %v, want [4]", got)
	}
	if got := blockSeqs(plan.Regenerate()); !equalSeqs(got, 2) {
		t.Fatalf("regenerate = %v, want [2]", got)
	}

	// The conflicting block cannot be spliced: the candidate keeps a gap.
	var out bytes.Buffer
	report, err := RunRepair(context.Background(), &out, bytes.NewReader(archive),
		bytes.NewReader(changed), int64(len(changed)), plan, opts, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if report.Complete {
		t.Fatal("candidate with a conflict must not be complete")
	}
	if len(report.Gaps) != 1 || report.Gaps[0].Seq != 4 {
		t.Fatalf("gaps = %+v, want one gap at block 4", report.Gaps)
	}
}

func TestMergeRepairPlansConflictAcrossInputVersions(t *testing.T) {
	blockSize := 64
	input := testInput(8, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}
	archive := compressToBytes(t, input, opts)
	corruptBlockPayload(t, archive, 3)

	// Two parallel tasks over overlapping ranges with the same input merge
	// and keep a single copy of the overlapping block.
	planA, err := PlanRepair(bytes.NewReader(archive), int64(len(archive)),
		bytes.NewReader(input), int64(len(input)), 0, int64(5*blockSize), opts)
	if err != nil {
		t.Fatal(err)
	}
	planB, err := PlanRepair(bytes.NewReader(archive), int64(len(archive)),
		bytes.NewReader(input), int64(len(input)), int64(3*blockSize), int64(5*blockSize), opts)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := MergeRepairPlans(planA, planB)
	if err != nil {
		t.Fatalf("MergeRepairPlans() error = %v", err)
	}
	if merged.RangeStart != 0 || merged.RangeEnd != int64(8*blockSize) {
		t.Fatalf("merged range = [%d,%d), want [0,%d)", merged.RangeStart, merged.RangeEnd, 8*blockSize)
	}
	if len(merged.Blocks) != 8 {
		t.Fatalf("merged blocks = %d, want 8 (no duplicates)", len(merged.Blocks))
	}
	if got := blockSeqs(merged.Regenerate()); !equalSeqs(got, 3) {
		t.Fatalf("merged regenerate = %v, want [3]", got)
	}

	// A parallel task that regenerated block 3 from a different input
	// version cannot be spliced into the same file.
	other := append([]byte(nil), input...)
	other[3*blockSize] = 'Q'
	planC, err := PlanRepair(bytes.NewReader(archive), int64(len(archive)),
		bytes.NewReader(other), int64(len(other)), int64(2*blockSize), int64(2*blockSize), opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MergeRepairPlans(planA, planC); !errors.Is(err, ErrRepairConflict) {
		t.Fatalf("MergeRepairPlans() error = %v, want ErrRepairConflict", err)
	}
}

func TestRepairFileGapThenFillOnReopen(t *testing.T) {
	blockSize := 64
	input := testInput(8, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}
	archive := compressToBytes(t, input, opts)
	corruptBlockPayload(t, archive, 2)
	corruptBlockPayload(t, archive, 5)

	dir := t.TempDir()
	path := filepath.Join(dir, "data.gcbk")
	if err := os.WriteFile(path, archive, 0o644); err != nil {
		t.Fatal(err)
	}

	// The input only covers the first corruption: deliver a gapped candidate.
	short := input[:4*blockSize]
	report, err := RepairFile(context.Background(), path,
		bytes.NewReader(short), int64(len(short)), 0, int64(len(input)), opts)
	if err != nil {
		t.Fatalf("RepairFile() error = %v", err)
	}
	if report.Complete || report.ReplaceOriginal {
		t.Fatal("candidate with missing input must not replace the original")
	}
	if len(report.Gaps) != 1 || report.Gaps[0].Seq != 5 {
		t.Fatalf("gaps = %+v, want one gap at block 5", report.Gaps)
	}
	if _, err := os.Stat(path + ".repairstate"); err != nil {
		t.Fatal("repair state should persist while gaps remain")
	}

	// Reopening with the full input resumes from the confirmed prefix and
	// finishes the candidate.
	report, err = RepairFile(context.Background(), path,
		bytes.NewReader(input), int64(len(input)), 0, int64(len(input)), opts)
	if err != nil {
		t.Fatalf("RepairFile() reopen error = %v", err)
	}
	if !report.Complete || !report.ReplaceOriginal {
		t.Fatalf("after reopen Complete=%v ReplaceOriginal=%v, want both true", report.Complete, report.ReplaceOriginal)
	}
	candidate, err := os.ReadFile(path + ".repair")
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeAll(t, candidate, nil); !bytes.Equal(got, input) {
		t.Fatal("final candidate does not decode to the original input")
	}
}

func TestRepairFileRejectsStaleInputVersion(t *testing.T) {
	blockSize := 64
	input := testInput(8, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}
	archive := compressToBytes(t, input, opts)
	corruptBlockPayload(t, archive, 2)

	dir := t.TempDir()
	path := filepath.Join(dir, "data.gcbk")
	if err := os.WriteFile(path, archive, 0o644); err != nil {
		t.Fatal(err)
	}

	// First run cannot finish: block 5 is damaged too, beyond the short input.
	corruptBlockPayload(t, archive, 5)
	if err := os.WriteFile(path, archive, 0o644); err != nil {
		t.Fatal(err)
	}
	short := input[:4*blockSize]
	if _, err := RepairFile(context.Background(), path,
		bytes.NewReader(short), int64(len(short)), 0, int64(len(input)), opts); err != nil {
		t.Fatal(err)
	}

	// A different input version that no longer covers the confirmed
	// regenerated block 2 must be rejected, not spliced.
	other := append([]byte(nil), input...)
	other[0] = 'Z'
	other = other[:2*blockSize]
	if _, err := RepairFile(context.Background(), path,
		bytes.NewReader(other), int64(len(other)), 0, int64(len(input)), opts); !errors.Is(err, ErrRepairConflict) {
		t.Fatalf("RepairFile() error = %v, want ErrRepairConflict", err)
	}
}
