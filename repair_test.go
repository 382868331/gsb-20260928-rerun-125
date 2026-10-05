package datacompression

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const repairBlockSize = 1024

func repairInput(blocks int) []byte {
	var buf bytes.Buffer
	for i := 0; i < blocks; i++ {
		buf.Write(bytes.Repeat([]byte{byte('A' + i%26)}, repairBlockSize))
	}
	return buf.Bytes()
}

func compressToFile(t *testing.T, path string, input []byte) {
	t.Helper()
	if err := CompressFile(context.Background(), path, bytes.NewReader(input), Options{BlockSize: repairBlockSize}); err != nil {
		t.Fatalf("CompressFile() error = %v", err)
	}
}

// damageBlocks flips one payload byte inside each listed block's frame.
func damageBlocks(t *testing.T, path string, seqs ...int) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderFromBytes(data, nil)
	if err != nil {
		t.Fatalf("NewReaderFromBytes() error = %v", err)
	}
	for _, seq := range seqs {
		entry := reader.Entries()[seq]
		data[entry.DataOffset+int64(frameHeaderSize)] ^= 0xff
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func readFileBytes(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func fullRange(blocks int) []RepairRange {
	return []RepairRange{{Start: 0, End: blocks}}
}

// Two damaged blocks, input only covers one of them: the plan must split
// the request into reuse, regenerate and missing-input before anything is
// written, and the repair must deliver a candidate that carries the gap.
func TestRepairPlanAndGapCandidate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.dc")
	input := repairInput(8)
	compressToFile(t, path, input)
	damageBlocks(t, path, 2, 5)
	original := readFileBytes(t, path)

	// Input stops after block 3, so damaged block 5 has no original input.
	short := input[:4*repairBlockSize]
	plan, manifest, err := RepairFile(context.Background(), path, bytes.NewReader(short), Options{BlockSize: repairBlockSize}, RepairTask{Ranges: fullRange(8)})
	if err != nil {
		t.Fatalf("RepairFile() error = %v", err)
	}
	if got := plan.Reusable(); !reflect.DeepEqual(got, []int{0, 1, 3, 4, 6, 7}) {
		t.Fatalf("Reusable() = %v", got)
	}
	if got := plan.Regenerate(); !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("Regenerate() = %v", got)
	}
	if got := plan.MissingInput(); !reflect.DeepEqual(got, []int{5}) {
		t.Fatalf("MissingInput() = %v", got)
	}
	if !manifest.Gap(5) {
		t.Fatalf("manifest.Gaps = %v, want block 5", manifest.Gaps)
	}

	// The original is untouched and the candidate is kept next to it.
	if got := readFileBytes(t, path); !bytes.Equal(got, original) {
		t.Fatal("original archive modified by repair")
	}
	if _, err := os.Stat(path + ".repair"); err != nil {
		t.Fatalf("candidate missing: %v", err)
	}

	// A gap-carrying candidate cannot replace the original.
	bundle, err := LoadRepairBundle(path)
	if err != nil {
		t.Fatalf("LoadRepairBundle() error = %v", err)
	}
	if _, err := CommitRepairFile(context.Background(), path, Options{BlockSize: repairBlockSize}, bundle); !errors.Is(err, ErrRepairIncomplete) {
		t.Fatalf("CommitRepairFile() error = %v, want ErrRepairIncomplete", err)
	}
	if got := readFileBytes(t, path); !bytes.Equal(got, original) {
		t.Fatal("original archive modified by rejected commit")
	}
}

// Full input repairs both damaged blocks; the commit replaces the original,
// the result decodes end to end, and the proof shows the bytes outside the
// requested range were copied unchanged.
func TestRepairCommitReplacesOriginal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.dc")
	input := repairInput(8)
	compressToFile(t, path, input)
	damageBlocks(t, path, 2, 5)
	corrupt := readFileBytes(t, path)

	repairTask := RepairTask{Ranges: []RepairRange{{Start: 2, End: 6}}}
	if _, _, err := RepairFile(context.Background(), path, bytes.NewReader(input), Options{BlockSize: repairBlockSize}, repairTask); err != nil {
		t.Fatalf("RepairFile() error = %v", err)
	}
	bundle, err := LoadRepairBundle(path)
	if err != nil {
		t.Fatalf("LoadRepairBundle() error = %v", err)
	}
	proof, err := CommitRepairFile(context.Background(), path, Options{BlockSize: repairBlockSize}, bundle)
	if err != nil {
		t.Fatalf("CommitRepairFile() error = %v", err)
	}
	if got := proof.Regenerated; !reflect.DeepEqual(got, []int{2, 5}) {
		t.Fatalf("proof.Regenerated = %v", got)
	}
	if proof.UnchangedBlocks != 6 || proof.RewrittenBlocks != 2 {
		t.Fatalf("proof = unchanged %d rewritten %d", proof.UnchangedBlocks, proof.RewrittenBlocks)
	}

	committed := readFileBytes(t, path)
	if got := decodeAll(t, committed, nil); !bytes.Equal(got, input) {
		t.Fatal("committed archive does not decode to the original input")
	}
	// The committed archive equals the pristine pre-damage archive byte for
	// byte: nothing outside the damaged frames was rewritten.
	if !bytes.Equal(committed, pristineArchive(t, input)) {
		t.Fatal("committed archive differs from a fresh compression of the same input")
	}
	if len(proof.OutsideRange) == 0 {
		t.Fatal("proof.OutsideRange is empty")
	}
	for _, span := range proof.OutsideRange {
		if span.Offset < 0 || span.Offset+span.Length > int64(len(corrupt)) {
			t.Fatalf("outside span %+v out of bounds", span)
		}
		if !bytes.Equal(committed[span.Offset:span.Offset+span.Length], corrupt[span.Offset:span.Offset+span.Length]) {
			t.Fatalf("outside span %+v was rewritten", span)
		}
	}
	if _, err := os.Stat(path + ".repair"); !os.IsNotExist(err) {
		t.Fatalf("candidate should be removed after commit, stat err = %v", err)
	}
}

func pristineArchive(t *testing.T, input []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := Compress(context.Background(), &buf, bytes.NewReader(input), Options{BlockSize: repairBlockSize}); err != nil {
		t.Fatalf("Compress() error = %v", err)
	}
	return buf.Bytes()
}

// Input from a different version for the damaged block is rejected as a
// conflict: the block becomes a gap, never spliced into the candidate.
func TestRepairInputVersionConflictStaysGap(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.dc")
	input := repairInput(6)
	compressToFile(t, path, input)
	damageBlocks(t, path, 3)
	original := readFileBytes(t, path)

	changed := repairInput(6)
	copy(changed[3*repairBlockSize:], bytes.Repeat([]byte{'z'}, repairBlockSize))
	plan, manifest, err := RepairFile(context.Background(), path, bytes.NewReader(changed), Options{BlockSize: repairBlockSize}, RepairTask{Ranges: fullRange(6)})
	if err != nil {
		t.Fatalf("RepairFile() error = %v", err)
	}
	if got := plan.InputConflicts(); !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("InputConflicts() = %v", got)
	}
	if !manifest.Gap(3) {
		t.Fatalf("manifest.Gaps = %v, want block 3", manifest.Gaps)
	}
	bundle, err := LoadRepairBundle(path)
	if err != nil {
		t.Fatalf("LoadRepairBundle() error = %v", err)
	}
	if _, err := CommitRepairFile(context.Background(), path, Options{BlockSize: repairBlockSize}, bundle); !errors.Is(err, ErrRepairIncomplete) {
		t.Fatalf("CommitRepairFile() error = %v, want ErrRepairIncomplete", err)
	}
	if got := readFileBytes(t, path); !bytes.Equal(got, original) {
		t.Fatal("original archive modified")
	}
}

// Two repair tasks over adjacent, overlapping ranges merge into one
// candidate; the overlapping blocks are adopted once, and the merged
// candidate commits into a complete archive.
func TestRepairMergeOverlappingTasks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.dc")
	input := repairInput(10)
	compressToFile(t, path, input)
	damageBlocks(t, path, 2, 7)

	taskA := RepairTask{Ranges: []RepairRange{{Start: 0, End: 5}}}
	taskB := RepairTask{Ranges: []RepairRange{{Start: 3, End: 10}}}
	// Run both tasks into their own candidates on disk copies.
	pathA := filepath.Join(dir, "a.dc")
	pathB := filepath.Join(dir, "b.dc")
	for _, p := range []string{pathA, pathB} {
		if err := os.WriteFile(p, readFileBytes(t, path), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := RepairFile(context.Background(), pathA, bytes.NewReader(input), Options{BlockSize: repairBlockSize}, taskA); err != nil {
		t.Fatalf("RepairFile(A) error = %v", err)
	}
	if _, _, err := RepairFile(context.Background(), pathB, bytes.NewReader(input), Options{BlockSize: repairBlockSize}, taskB); err != nil {
		t.Fatalf("RepairFile(B) error = %v", err)
	}
	bundleA, err := LoadRepairBundle(pathA)
	if err != nil {
		t.Fatal(err)
	}
	bundleB, err := LoadRepairBundle(pathB)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := MergeRepairBundles(bundleA, bundleB)
	if err != nil {
		t.Fatalf("MergeRepairBundles() error = %v", err)
	}
	seen := map[int]int{}
	for _, seq := range merged.RepairedSeqs() {
		seen[seq]++
	}
	for seq, n := range seen {
		if n != 1 {
			t.Fatalf("block %d adopted %d times", seq, n)
		}
	}
	if len(merged.Gaps) != 0 {
		t.Fatalf("merged.Gaps = %v", merged.Gaps)
	}
	proof, err := CommitRepairFile(context.Background(), path, Options{BlockSize: repairBlockSize}, bundleA, bundleB)
	if err != nil {
		t.Fatalf("CommitRepairFile() error = %v", err)
	}
	if got := proof.Regenerated; !reflect.DeepEqual(got, []int{2, 7}) {
		t.Fatalf("proof.Regenerated = %v", got)
	}
	if got := decodeAll(t, readFileBytes(t, path), nil); !bytes.Equal(got, input) {
		t.Fatal("merged commit does not decode to the original input")
	}
}

// Candidates produced from different input versions disagree on the
// overlapping block and must never be assembled into one file.
func TestRepairMergeConflictingVersionsRejected(t *testing.T) {
	base := RepairManifest{
		Version:    formatVersion,
		BlockCount: 4,
		BlockSize:  repairBlockSize,
		Ranges:     []RepairRange{{Start: 0, End: 3}},
		Frames:     []RepairFrame{{Seq: 1, Offset: headerSize, Length: 40, Checksum: 111}},
	}
	other := base
	other.Ranges = []RepairRange{{Start: 1, End: 4}}
	other.Frames = []RepairFrame{{Seq: 1, Offset: headerSize, Length: 40, Checksum: 222}}
	if _, err := MergeRepairManifests(base, other); !errors.Is(err, ErrRepairConflict) {
		t.Fatalf("MergeRepairManifests() error = %v, want ErrRepairConflict", err)
	}
}

// Cancellation keeps the confirmed frames; resuming continues from them
// instead of restarting, and the finished candidate commits cleanly.
func TestRepairCancelAndResume(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.dc")
	input := repairInput(64)
	compressToFile(t, path, input)
	damageBlocks(t, path, 4, 9)

	ctx, cancel := context.WithCancel(context.Background())
	saves := 0
	_, _, err := RepairFileResume(ctx, path, bytes.NewReader(input), Options{BlockSize: repairBlockSize}, RepairTask{Ranges: fullRange(64)}, false)
	_ = err
	// Cancel partway through a manual run using the save hook.
	var buf bytes.Buffer
	saves = 0
	src := readFileBytes(t, path)
	_, partial, err := Repair(ctx, &buf, bytes.NewReader(src), int64(len(src)), Options{BlockSize: repairBlockSize}, RepairTask{Ranges: fullRange(64), Input: bytes.NewReader(input)}, func(RepairManifest) error {
		saves++
		if saves == 3 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Repair() error = %v, want context.Canceled", err)
	}
	confirmed := len(partial.Frames)
	if confirmed == 0 || confirmed >= 64 {
		t.Fatalf("confirmed frames = %d, want a strict prefix", confirmed)
	}

	// File-level: interrupt, then resume from the confirmed manifest.
	cancelPath := filepath.Join(dir, "cancel.dc")
	if err := os.WriteFile(cancelPath, src, 0o644); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if _, _, err := RepairFile(ctx2, cancelPath, bytes.NewReader(input), Options{BlockSize: repairBlockSize}, RepairTask{Ranges: fullRange(64)}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RepairFile(canceled) error = %v, want context.Canceled", err)
	}
	_, manifest, err := ResumeRepairFile(context.Background(), cancelPath, bytes.NewReader(input), Options{BlockSize: repairBlockSize}, RepairTask{Ranges: fullRange(64)})
	if err != nil {
		t.Fatalf("ResumeRepairFile() error = %v", err)
	}
	if len(manifest.Gaps) != 0 {
		t.Fatalf("manifest.Gaps = %v", manifest.Gaps)
	}
	bundle, err := LoadRepairBundle(cancelPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CommitRepairFile(context.Background(), cancelPath, Options{BlockSize: repairBlockSize}, bundle); err != nil {
		t.Fatalf("CommitRepairFile() error = %v", err)
	}
	if got := decodeAll(t, readFileBytes(t, cancelPath), nil); !bytes.Equal(got, input) {
		t.Fatal("resumed commit does not decode to the original input")
	}
}
