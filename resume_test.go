package datacompression

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// failAfterReader returns err after limit bytes have been delivered.
type failAfterReader struct {
	data  []byte
	limit int
	err   error
}

func (r *failAfterReader) Read(p []byte) (int, error) {
	if r.limit <= 0 {
		return 0, r.err
	}
	if len(p) > r.limit {
		p = p[:r.limit]
	}
	n := copy(p, r.data[:len(p)])
	r.data = r.data[n:]
	r.limit -= n
	return n, nil
}

func testInput(blocks, blockSize int) []byte {
	var buf bytes.Buffer
	for i := 0; i < blocks; i++ {
		buf.Write(bytes.Repeat([]byte{byte('a' + i%26)}, blockSize))
	}
	return buf.Bytes()
}

func decodeAll(t *testing.T, data []byte, codecs []Codec) []byte {
	t.Helper()
	reader, err := NewReaderFromBytes(data, codecs)
	if err != nil {
		t.Fatalf("NewReaderFromBytes() error = %v", err)
	}
	var out bytes.Buffer
	for i := range reader.Entries() {
		block, err := reader.ReadBlock(i)
		if err != nil {
			t.Fatalf("ReadBlock(%d) error = %v", i, err)
		}
		out.Write(block)
	}
	return out.Bytes()
}

func TestResumeMatchesOneShotAcrossConcurrency(t *testing.T) {
	blockSize := 64
	input := testInput(12, blockSize)
	opts := Options{BlockSize: blockSize, Concurrency: 2}

	var oneShot bytes.Buffer
	if err := Compress(context.Background(), &oneShot, bytes.NewReader(input), opts); err != nil {
		t.Fatal(err)
	}

	// Interrupt deterministically: the input reader fails after 5 blocks.
	readErr := errors.New("read failed")
	var partial bytes.Buffer
	var saved Checkpoint
	hook := func(cp Checkpoint) error {
		saved = cp
		return nil
	}
	err := CompressWithCheckpoint(context.Background(), &partial,
		&failAfterReader{data: input, limit: 5 * blockSize, err: readErr}, opts, hook)
	if !errors.Is(err, readErr) {
		t.Fatalf("CompressWithCheckpoint() error = %v, want %v", err, readErr)
	}
	confirmed := len(saved.Entries)
	if confirmed < 4 || confirmed > 5 || !saved.Interrupted {
		t.Fatalf("checkpoint entries = %d interrupted = %v, want 5 interrupted entries",
			confirmed, saved.Interrupted)
	}
	if saved.InputOffset != int64(confirmed*blockSize) || saved.OutputOffset != int64(partial.Len()) {
		t.Fatalf("checkpoint offsets = (%d, %d), partial len = %d",
			saved.InputOffset, saved.OutputOffset, partial.Len())
	}

	// The confirmed prefix must be verifiably readable from the partial output.
	pr, err := NewPartialReader(bytes.NewReader(partial.Bytes()), int64(partial.Len()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pr.Complete() {
		t.Fatal("partial output reported complete")
	}
	if !errors.Is(pr.TailError(), ErrIncomplete) {
		t.Fatalf("TailError() = %v, want ErrIncomplete", pr.TailError())
	}
	if len(pr.Entries()) != confirmed || pr.ConfirmedOffset() != int64(confirmed*blockSize) {
		t.Fatalf("confirmed entries = %d offset = %d", len(pr.Entries()), pr.ConfirmedOffset())
	}
	if _, err := pr.ReadBlock(confirmed); !errors.Is(err, ErrBlockNotFound) {
		t.Fatalf("ReadBlock(%d) error = %v, want ErrBlockNotFound", confirmed, err)
	}

	for _, concurrency := range []int{1, 3, 8} {
		resumed := bytes.NewBuffer(append([]byte(nil), partial.Bytes()...))
		if err := Resume(context.Background(), resumed, bytes.NewReader(input),
			Options{BlockSize: blockSize, Concurrency: concurrency}, saved, nil); err != nil {
			t.Fatalf("Resume(concurrency=%d) error = %v", concurrency, err)
		}
		if got := decodeAll(t, resumed.Bytes(), nil); !bytes.Equal(got, input) {
			t.Fatalf("Resume(concurrency=%d) content mismatch", concurrency)
		}
		if !bytes.Equal(resumed.Bytes(), oneShot.Bytes()) {
			t.Fatalf("Resume(concurrency=%d) archive differs from one-shot", concurrency)
		}
	}
}

func TestResumeRejectsIncompatibleStateAndKeepsEvidence(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out.gcbk")
	blockSize := 32
	input := testInput(8, blockSize)
	readErr := errors.New("boom")

	err := CompressFileResumable(context.Background(), target,
		&failAfterReader{data: input, limit: 3 * blockSize, err: readErr},
		Options{BlockSize: blockSize, Concurrency: 2})
	if !errors.Is(err, readErr) {
		t.Fatalf("CompressFileResumable() error = %v, want %v", err, readErr)
	}
	partialBytes, err := os.ReadFile(partialPath(target))
	if err != nil {
		t.Fatalf("partial output missing after interruption: %v", err)
	}
	checkpointBytes, err := os.ReadFile(checkpointPath(target))
	if err != nil {
		t.Fatalf("checkpoint missing after interruption: %v", err)
	}
	cp, err := LoadCheckpoint(checkpointPath(target))
	if err != nil {
		t.Fatal(err)
	}
	if !cp.Interrupted || cp.Failure == "" {
		t.Fatalf("checkpoint does not record interruption evidence: %+v", cp)
	}

	// Changed input content must be rejected.
	changed := append([]byte(nil), input...)
	changed[0] ^= 0xff
	if err := ResumeFile(context.Background(), target, bytes.NewReader(changed),
		Options{BlockSize: blockSize, Concurrency: 2}); !errors.Is(err, ErrIncompatibleState) {
		t.Fatalf("ResumeFile(changed input) error = %v, want ErrIncompatibleState", err)
	}

	// Changed encoding settings must be rejected.
	if err := ResumeFile(context.Background(), target, bytes.NewReader(input),
		Options{BlockSize: blockSize * 2, Concurrency: 2}); !errors.Is(err, ErrIncompatibleState) {
		t.Fatalf("ResumeFile(changed block size) error = %v, want ErrIncompatibleState", err)
	}

	// Original output and interruption evidence must be preserved.
	if got, _ := os.ReadFile(partialPath(target)); !bytes.Equal(got, partialBytes) {
		t.Fatal("partial output modified by rejected resume")
	}
	if got, _ := os.ReadFile(checkpointPath(target)); !bytes.Equal(got, checkpointBytes) {
		t.Fatal("checkpoint modified by rejected resume")
	}

	// Compatible resume finishes and matches a one-shot run.
	if err := ResumeFile(context.Background(), target, bytes.NewReader(input),
		Options{BlockSize: blockSize, Concurrency: 3}); err != nil {
		t.Fatalf("ResumeFile() error = %v", err)
	}
	finalBytes, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if got := decodeAll(t, finalBytes, nil); !bytes.Equal(got, input) {
		t.Fatal("resumed file content mismatch")
	}
	if _, err := os.Stat(checkpointPath(target)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checkpoint should be removed after success, err = %v", err)
	}
}

func TestPartialReaderRejectsTruncatedAndCorruptTail(t *testing.T) {
	blockSize := 48
	input := testInput(6, blockSize)
	var archive bytes.Buffer
	if err := Compress(context.Background(), &archive, bytes.NewReader(input),
		Options{BlockSize: blockSize, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	valid := archive.Bytes()

	full, err := NewPartialReader(bytes.NewReader(valid), int64(len(valid)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !full.Complete() || full.TailError() != nil || len(full.Entries()) != 6 {
		t.Fatalf("full archive: complete=%v tail=%v entries=%d",
			full.Complete(), full.TailError(), len(full.Entries()))
	}

	// Truncate inside the 5th frame: only 4 blocks may be confirmed.
	entries := full.Entries()
	cut := int(entries[4].DataOffset) + frameHeaderSize + 3
	truncated, err := NewPartialReader(bytes.NewReader(valid[:cut]), int64(cut), nil)
	if err != nil {
		t.Fatal(err)
	}
	if truncated.Complete() {
		t.Fatal("truncated archive reported complete")
	}
	if !errors.Is(truncated.TailError(), ErrIncomplete) {
		t.Fatalf("TailError() = %v, want ErrIncomplete", truncated.TailError())
	}
	if len(truncated.Entries()) != 4 {
		t.Fatalf("confirmed entries after truncation = %d, want 4", len(truncated.Entries()))
	}
	got, err := truncated.ReadBlock(3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, input[3*blockSize:4*blockSize]) {
		t.Fatal("confirmed block content mismatch")
	}

	// Corrupt the payload of the last frame: it must not count as complete.
	corrupt := append([]byte(nil), valid...)
	corrupt[int(entries[5].DataOffset)+frameHeaderSize] ^= 0xff
	damaged, err := NewPartialReader(bytes.NewReader(corrupt), int64(len(corrupt)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if damaged.Complete() {
		t.Fatal("corrupt-tail archive reported complete")
	}
	if len(damaged.Entries()) != 5 {
		t.Fatalf("confirmed entries with corrupt tail = %d, want 5", len(damaged.Entries()))
	}
	if !errors.Is(damaged.TailError(), ErrBadChecksum) && !errors.Is(damaged.TailError(), ErrBadArchive) {
		t.Fatalf("TailError() = %v, want checksum/archive corruption", damaged.TailError())
	}
}

func TestResumeRespectsInFlightWindow(t *testing.T) {
	concurrency := 4
	blockSize := 16
	blocks := testBlocks(60, blockSize, blockSize)
	input := bytes.Join(blocks, nil)
	readErr := errors.New("stop")

	tracker := &backlogTracker{}
	var partial bytes.Buffer
	var saved Checkpoint
	codec := boundedCodec{}
	err := CompressWithCheckpoint(context.Background(), &partial,
		&failAfterReader{data: input, limit: 10 * blockSize, err: readErr},
		Options{
			BlockSize:   blockSize,
			Concurrency: concurrency,
			Codecs:      []Codec{storeCodec{}, codec},
			Selector:    func(Block, []Codec) Codec { return codec },
		},
		func(cp Checkpoint) error { saved = cp; return nil })
	if !errors.Is(err, readErr) {
		t.Fatalf("CompressWithCheckpoint() error = %v", err)
	}

	// Resume with the first remaining block gated: the in-flight window must
	// stay within the concurrency limit while it is blocked.
	release := make(chan struct{})
	resumed := bytes.NewBuffer(append([]byte(nil), partial.Bytes()...))
	done := make(chan error, 1)
	go func() {
		done <- Resume(context.Background(), &trackingWriter{w: resumed, tracker: tracker},
			&blockReader{blocks: blocks, tracker: tracker, untracked: len(saved.Entries)},
			Options{
				BlockSize:   blockSize,
				Concurrency: concurrency,
				Codecs:      []Codec{storeCodec{}, codec},
				Selector: func(block Block, _ []Codec) Codec {
					if block.Seq == len(saved.Entries) {
						return gatedCodec{codec: codec, release: release}
					}
					return codec
				},
			}, saved, nil)
	}()

	deadline := time.Now().Add(time.Second)
	for tracker.peak() < concurrency && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := tracker.peak(); got != concurrency {
		t.Fatalf("peak backlog before gated block published = %d, want %d", got, concurrency)
	}
	time.Sleep(20 * time.Millisecond)
	if got := tracker.peak(); got > concurrency {
		t.Fatalf("peak backlog while blocked = %d, want at most %d", got, concurrency)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := tracker.peak(); got > concurrency {
		t.Fatalf("peak backlog = %d, want at most %d", got, concurrency)
	}
	if got := decodeAll(t, resumed.Bytes(), []Codec{codec}); !bytes.Equal(got, input) {
		t.Fatal("resumed content mismatch")
	}
}

func TestResumeCancellationKeepsWindowAndEvidence(t *testing.T) {
	blockSize := 16
	input := testInput(40, blockSize)
	readErr := errors.New("stop")
	var partial bytes.Buffer
	var saved Checkpoint
	codec := blockingCodec{}
	if err := CompressWithCheckpoint(context.Background(), &partial,
		&failAfterReader{data: input, limit: 4 * blockSize, err: readErr},
		Options{
			BlockSize:   blockSize,
			Concurrency: 2,
			Codecs:      []Codec{storeCodec{}, codec},
			Selector:    func(Block, []Codec) Codec { return storeCodec{} },
		},
		func(cp Checkpoint) error { saved = cp; return nil }); !errors.Is(err, readErr) {
		t.Fatalf("CompressWithCheckpoint() error = %v", err)
	}

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	codec.release = release
	ctx, cancel := context.WithCancel(context.Background())
	resumed := bytes.NewBuffer(append([]byte(nil), partial.Bytes()...))
	done := make(chan error, 1)
	go func() {
		done <- Resume(ctx, resumed, bytes.NewReader(input), Options{
			BlockSize:   blockSize,
			Concurrency: 2,
			Codecs:      []Codec{storeCodec{}, codec},
			Selector:    func(Block, []Codec) Codec { return codec },
		}, saved, nil)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Resume() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Resume did not return after cancellation")
	}
	// No new frames may have been published after cancellation.
	if resumed.Len() != partial.Len() {
		t.Fatalf("cancelled resume wrote %d extra bytes", resumed.Len()-partial.Len())
	}
}

func TestPartialReaderOnStrings(t *testing.T) {
	input := strings.Repeat("checkpoint-", 100)
	var archive bytes.Buffer
	if err := Compress(context.Background(), &archive, strings.NewReader(input),
		Options{BlockSize: 55}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewPartialReader(bytes.NewReader(archive.Bytes()), int64(archive.Len()), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reader.Complete() {
		t.Fatal("complete archive not detected")
	}
	var out bytes.Buffer
	for i := range reader.Entries() {
		block, err := reader.ReadBlock(i)
		if err != nil {
			t.Fatal(err)
		}
		out.Write(block)
	}
	if out.String() != input {
		t.Fatal("partial reader round trip mismatch")
	}
}

var _ io.Reader = (*failAfterReader)(nil)
