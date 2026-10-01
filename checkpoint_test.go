package datacompression

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type cancelAfterWriter struct {
	w      io.Writer
	n      int64
	limit  int64
	cancel context.CancelFunc
}

func (w *cancelAfterWriter) Write(p []byte) (int, error) {
	n, err := w.w.Write(p)
	w.n += int64(n)
	if w.cancel != nil && w.n >= w.limit {
		w.cancel()
		w.cancel = nil
	}
	return n, err
}

func testInput(size int) []byte {
	input := make([]byte, size)
	for i := range input {
		input[i] = byte(i*31 + i/97)
	}
	// Mix in compressible stretches so both store and flate blocks appear.
	for i := 0; i+64 < len(input); i += 997 {
		for j := 0; j < 64; j++ {
			input[i+j] = 0
		}
	}
	return input
}

func decodeAll(t *testing.T, data []byte, codecs []Codec) []byte {
	t.Helper()
	reader, err := NewReaderFromBytes(data, codecs)
	if err != nil {
		t.Fatalf("NewReaderFromBytes() error = %v", err)
	}
	var out bytes.Buffer
	for _, entry := range reader.Entries() {
		block, err := reader.ReadBlock(entry.Seq)
		if err != nil {
			t.Fatalf("ReadBlock(%d) error = %v", entry.Seq, err)
		}
		out.Write(block)
	}
	return out.Bytes()
}

func TestResumeMatchesOneShotAcrossConcurrency(t *testing.T) {
	input := testInput(200 * 512)
	for _, concurrency := range []int{1, 2, 3, 5, 8} {
		t.Run(string(rune('0'+concurrency)), func(t *testing.T) {
			opts := Options{BlockSize: 512, Concurrency: concurrency}

			var oneShot bytes.Buffer
			if err := Compress(context.Background(), &oneShot, bytes.NewReader(input), opts); err != nil {
				t.Fatal(err)
			}

			// Interrupt the first run partway through.
			ctx, cancel := context.WithCancel(context.Background())
			var partial bytes.Buffer
			cw := &countingWriter{w: &cancelAfterWriter{w: &partial, limit: int64(oneShot.Len() / 3), cancel: cancel}}
			checkpoint, err := runCompression(ctx, cw, bytes.NewReader(input), opts, nil)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("first run error = %v, want context.Canceled", err)
			}
			if checkpoint == nil || checkpoint.NextSeq == 0 {
				t.Fatalf("checkpoint = %+v, want confirmed progress", checkpoint)
			}
			if int64(partial.Len()) < checkpoint.DataEnd {
				t.Fatalf("partial output %d bytes shorter than confirmed end %d", partial.Len(), checkpoint.DataEnd)
			}

			// Resume keeps only the confirmed prefix and appends the rest.
			resumed := bytes.NewBuffer(append([]byte(nil), partial.Bytes()[:checkpoint.DataEnd]...))
			resumeWriter := &countingWriter{w: resumed, n: checkpoint.DataEnd}
			if _, err := runCompression(context.Background(), resumeWriter, bytes.NewReader(input), opts, checkpoint); err != nil {
				t.Fatalf("resume error = %v", err)
			}
			if !bytes.Equal(resumed.Bytes(), oneShot.Bytes()) {
				t.Fatal("resumed archive differs from one-shot archive")
			}
			if got := decodeAll(t, resumed.Bytes(), nil); !bytes.Equal(got, input) {
				t.Fatal("resumed archive decodes to different content")
			}
		})
	}
}

func TestResumeRejectsIncompatibleStateAndKeepsEvidence(t *testing.T) {
	input := testInput(64 * 512)
	opts := Options{BlockSize: 512, Concurrency: 2}

	ctx, cancel := context.WithCancel(context.Background())
	var partial bytes.Buffer
	cw := &countingWriter{w: &cancelAfterWriter{w: &partial, limit: 4096, cancel: cancel}}
	checkpoint, err := runCompression(ctx, cw, bytes.NewReader(input), opts, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("first run error = %v", err)
	}

	// Changed input content must be rejected.
	changed := append([]byte(nil), input...)
	changed[17] ^= 0xff
	resumed := bytes.NewBuffer(append([]byte(nil), partial.Bytes()[:checkpoint.DataEnd]...))
	rw := &countingWriter{w: resumed, n: checkpoint.DataEnd}
	if _, err := runCompression(context.Background(), rw, bytes.NewReader(changed), opts, checkpoint); !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("changed input error = %v, want ErrIncompatibleCheckpoint", err)
	}

	// Changed encoding settings must be rejected.
	if err := checkpoint.CompatibleWith(Options{BlockSize: 256}); !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("changed block size error = %v, want ErrIncompatibleCheckpoint", err)
	}
	if err := checkpoint.CompatibleWith(Options{BlockSize: 512, Codecs: []Codec{storeCodec{}, futureCodec{}}}); !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("changed codec set error = %v, want ErrIncompatibleCheckpoint", err)
	}
	if err := checkpoint.CompatibleWith(opts); err != nil {
		t.Fatalf("same options rejected: %v", err)
	}
}

func TestCompressFileResumableAndResume(t *testing.T) {
	input := testInput(300 * 512)
	dir := t.TempDir()
	target := filepath.Join(dir, "large.gcbk")
	opts := Options{BlockSize: 512, Concurrency: 4}

	// Cancel once some confirmed output exists.
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CompressFileResumable(ctx, target, &slowReader{r: bytes.NewReader(input)}, opts)
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("CompressFileResumable error = %v, want context.Canceled", err)
	}

	// Original output must not exist yet; evidence must be preserved.
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target exists after interruption: %v", err)
	}
	checkpoint, err := LoadCheckpoint(CheckpointPath(target))
	if err != nil {
		t.Fatalf("LoadCheckpoint() error = %v", err)
	}
	if checkpoint.Reason == "" {
		t.Fatal("checkpoint missing interruption reason")
	}

	partial, err := OpenPartialReader(PartialPath(target), nil)
	if err != nil {
		t.Fatalf("OpenPartialReader() error = %v", err)
	}
	if partial.Complete() {
		t.Fatal("partial output reported complete")
	}
	if got := len(partial.CompleteBlocks()); got != checkpoint.NextSeq {
		t.Fatalf("confirmed blocks = %d, want %d", got, checkpoint.NextSeq)
	}
	if _, err := partial.ReadBlock(partial.NextSeq()); !errors.Is(err, ErrBlockNotFound) {
		t.Fatalf("unpublished block read error = %v, want ErrBlockNotFound", err)
	}
	partial.Close()

	// Resume with the same input and settings.
	if err := ResumeCompressFile(context.Background(), target, bytes.NewReader(input), opts); err != nil {
		t.Fatalf("ResumeCompressFile() error = %v", err)
	}
	if _, err := os.Stat(CheckpointPath(target)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("checkpoint file kept after successful resume")
	}
	if _, err := os.Stat(PartialPath(target)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("partial file kept after successful resume")
	}

	final, err := OpenReader(target, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	for _, entry := range final.Entries() {
		block, err := final.ReadBlock(entry.Seq)
		if err != nil {
			t.Fatal(err)
		}
		got.Write(block)
	}
	final.Close()
	if !bytes.Equal(got.Bytes(), input) {
		t.Fatal("resumed file content differs from input")
	}

	// Compare against a single uninterrupted run.
	oneShot := filepath.Join(dir, "oneshot.gcbk")
	if err := CompressFile(context.Background(), oneShot, bytes.NewReader(input), opts); err != nil {
		t.Fatal(err)
	}
	wantBytes, _ := os.ReadFile(oneShot)
	gotBytes, _ := os.ReadFile(target)
	if !bytes.Equal(gotBytes, wantBytes) {
		t.Fatal("resumed file bytes differ from one-shot encoding")
	}
}

func TestResumeFileRejectsChangedInputAndKeepsEvidence(t *testing.T) {
	input := testInput(64 * 512)
	dir := t.TempDir()
	target := filepath.Join(dir, "keep.gcbk")
	opts := Options{BlockSize: 512, Concurrency: 2}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- CompressFileResumable(ctx, target, &slowReader{r: bytes.NewReader(input)}, opts)
	}()
	time.Sleep(30 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	before, err := os.ReadFile(PartialPath(target))
	if err != nil {
		t.Fatal(err)
	}

	changed := append([]byte(nil), input...)
	changed[100] ^= 0xff
	if err := ResumeCompressFile(context.Background(), target, bytes.NewReader(changed), opts); !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("changed input error = %v, want ErrIncompatibleCheckpoint", err)
	}
	if err := ResumeCompressFile(context.Background(), target, bytes.NewReader(input), Options{BlockSize: 1024}); !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("changed options error = %v, want ErrIncompatibleCheckpoint", err)
	}

	// Evidence must be untouched.
	after, err := os.ReadFile(PartialPath(target))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("partial output modified after rejected resume")
	}
	if _, err := LoadCheckpoint(CheckpointPath(target)); err != nil {
		t.Fatal("checkpoint lost after rejected resume")
	}

	// A compatible resume still succeeds afterwards.
	if err := ResumeCompressFile(context.Background(), target, bytes.NewReader(input), opts); err != nil {
		t.Fatalf("resume after rejections error = %v", err)
	}
}

func TestPartialReaderTruncatedAndCorruptTail(t *testing.T) {
	parts := testBlocks(12, 100, 100)
	input := bytes.Join(parts, nil)
	var archive bytes.Buffer
	if err := Compress(context.Background(), &archive, bytes.NewReader(input), Options{BlockSize: 100, Concurrency: 1}); err != nil {
		t.Fatal(err)
	}
	valid := archive.Bytes()

	reader, err := NewReaderFromBytes(valid, nil)
	if err != nil {
		t.Fatal(err)
	}
	entries := reader.Entries()

	// Truncate in the middle of the 8th frame.
	cut := entries[7].DataOffset + frameHeaderSize + 3
	p, err := NewPartialReader(bytes.NewReader(valid[:cut]), int64(cut), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Complete() {
		t.Fatal("truncated archive reported complete")
	}
	if got := len(p.CompleteBlocks()); got != 7 {
		t.Fatalf("confirmed blocks = %d, want 7", got)
	}
	for i := 0; i < 7; i++ {
		block, err := p.ReadBlock(i)
		if err != nil {
			t.Fatalf("ReadBlock(%d) error = %v", i, err)
		}
		if !bytes.Equal(block, parts[i]) {
			t.Fatalf("ReadBlock(%d) mismatch", i)
		}
	}
	if _, err := p.ReadBlock(7); !errors.Is(err, ErrBlockNotFound) {
		t.Fatalf("truncated block error = %v, want ErrBlockNotFound", err)
	}

	// Corrupt the payload of the last frame: it must not count as complete.
	corrupt := append([]byte(nil), valid...)
	last := entries[len(entries)-1]
	corrupt[last.DataOffset+frameHeaderSize] ^= 0xff
	p2, err := NewPartialReader(bytes.NewReader(corrupt), int64(len(corrupt)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Complete() {
		t.Fatal("corrupt archive reported complete")
	}
	if got := len(p2.CompleteBlocks()); got != len(entries)-1 {
		t.Fatalf("confirmed blocks = %d, want %d", got, len(entries)-1)
	}
	if p2.StoppedAt() != last.DataOffset {
		t.Fatalf("stopped at %d, want %d", p2.StoppedAt(), last.DataOffset)
	}

	// A complete archive is reported complete with all blocks.
	p3, err := NewPartialReader(bytes.NewReader(valid), int64(len(valid)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !p3.Complete() || len(p3.CompleteBlocks()) != len(entries) {
		t.Fatal("complete archive not recognized")
	}
}

func TestRepeatedInterruptionsEventuallyComplete(t *testing.T) {
	input := testInput(120 * 512)
	opts := Options{BlockSize: 512, Concurrency: 3}

	var oneShot bytes.Buffer
	if err := Compress(context.Background(), &oneShot, bytes.NewReader(input), opts); err != nil {
		t.Fatal(err)
	}

	// Interrupt three times, then finish.
	var current bytes.Buffer
	var checkpoint *Checkpoint
	limit := int64(oneShot.Len() / 5)
	for attempt := 0; attempt < 3; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		sink := &cancelAfterWriter{w: &current, limit: limit, cancel: cancel}
		cw := &countingWriter{w: sink, n: int64(current.Len())}
		if checkpoint != nil {
			// Drop unconfirmed tail before resuming.
			current.Truncate(int(checkpoint.DataEnd))
			cw.n = checkpoint.DataEnd
		}
		var err error
		checkpoint, err = runCompression(ctx, cw, bytes.NewReader(input), opts, checkpoint)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("attempt %d error = %v, want context.Canceled", attempt, err)
		}
	}
	current.Truncate(int(checkpoint.DataEnd))
	cw := &countingWriter{w: &current, n: checkpoint.DataEnd}
	if _, err := runCompression(context.Background(), cw, bytes.NewReader(input), opts, checkpoint); err != nil {
		t.Fatalf("final resume error = %v", err)
	}
	if !bytes.Equal(current.Bytes(), oneShot.Bytes()) {
		t.Fatal("archive after repeated interruptions differs from one-shot")
	}
}

type slowReader struct{ r io.Reader }

func (s *slowReader) Read(p []byte) (int, error) {
	time.Sleep(time.Millisecond)
	return s.r.Read(p)
}
