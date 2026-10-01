package datacompression

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	ErrBadArchive      = errors.New("invalid archive")
	ErrBadChecksum     = errors.New("archive checksum mismatch")
	ErrUnknownCodec    = errors.New("unknown codec")
	ErrBlockNotFound   = errors.New("block not found")
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrIncompatibleCheckpoint is returned when input data or encoding
	// settings no longer match a saved checkpoint.
	ErrIncompatibleCheckpoint = errors.New("incompatible checkpoint")
	// ErrCheckpointNotFound is returned when no resumable state exists.
	ErrCheckpointNotFound = errors.New("checkpoint not found")
)

type Block struct {
	Seq            int
	OriginalOffset int64
	Data           []byte
}

type Entry struct {
	Seq              int
	OriginalOffset   int64
	OriginalLength   int64
	DataOffset       int64
	CompressedLength int64
	Codec            uint16
	Checksum         uint32
}

type Options struct {
	BlockSize   int
	Concurrency int
	Codecs      []Codec
	Selector    Selector
}

func (o Options) withDefaults() Options {
	if o.BlockSize <= 0 {
		o.BlockSize = 64 * 1024
	}
	if o.Concurrency <= 0 {
		o.Concurrency = 4
	}
	if len(o.Codecs) == 0 {
		o.Codecs = DefaultCodecs()
	}
	if o.Selector == nil {
		o.Selector = DefaultSelector
	}
	return o
}

type resultBlock struct {
	seq      int
	offset   int64
	data     []byte
	codec    uint16
	payload  []byte
	checksum uint32
	err      error
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

func Compress(ctx context.Context, w io.Writer, r io.Reader, opts Options) error {
	cw := &countingWriter{w: w}
	_, err := runCompression(ctx, cw, r, opts, nil)
	return err
}

// runCompression executes the encode pipeline. When resume is nil it writes a
// fresh archive starting with the header; otherwise it verifies the input
// prefix against the checkpoint and continues numbering after the confirmed
// blocks, assuming cw is already positioned at resume.DataEnd. On failure it
// returns a checkpoint describing the confirmed progress so far.
func runCompression(ctx context.Context, cw *countingWriter, r io.Reader, opts Options, resume *Checkpoint) (*Checkpoint, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts = opts.withDefaults()
	if cw == nil || cw.w == nil || r == nil || opts.BlockSize <= 0 || opts.Concurrency <= 0 || opts.Selector == nil {
		return nil, ErrInvalidArgument
	}
	if codecByID(opts.Codecs, CodecStore) == nil {
		opts.Codecs = append(opts.Codecs, storeCodec{})
	}

	entries := make([]Entry, 0)
	startSeq := 0
	var startOffset int64
	if resume == nil {
		if err := writeHeader(cw); err != nil {
			return nil, err
		}
	} else {
		entries = append(entries, resume.Entries...)
		startSeq = resume.NextSeq
		startOffset = resume.NextOffset
	}

	jobs := make(chan Block)
	results := make(chan resultBlock, opts.Concurrency)
	published := make(chan struct{}, opts.Concurrency)
	var workers sync.WaitGroup
	var reader sync.WaitGroup
	workers.Add(opts.Concurrency)

	for worker := 0; worker < opts.Concurrency; worker++ {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case block, ok := <-jobs:
					if !ok {
						return
					}
					result := encodeBlock(block, opts)
					select {
					case <-ctx.Done():
						return
					case results <- result:
					}
				}
			}
		}()
	}

	go func() {
		workers.Wait()
		close(results)
	}()

	reader.Add(1)
	go func() {
		defer reader.Done()
		defer close(jobs)
		// Re-read and verify the already published prefix without
		// republishing it; any mismatch rejects the resume.
		for i := 0; i < startSeq; i++ {
			buf := make([]byte, opts.BlockSize)
			n, readErr := io.ReadFull(r, buf)
			entry := entries[i]
			if n == 0 || int64(n) != entry.OriginalLength || crc32Checksum(buf[:n]) != entry.Checksum {
				select {
				case <-ctx.Done():
				case results <- resultBlock{seq: startSeq, offset: startOffset, err: ErrIncompatibleCheckpoint}:
				}
				return
			}
			if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
				select {
				case <-ctx.Done():
				case results <- resultBlock{seq: startSeq, offset: startOffset, err: readErr}:
				}
				return
			}
		}
		seq := startSeq
		offset := startOffset
		inFlight := 0
		for {
			if inFlight >= opts.Concurrency {
				select {
				case <-ctx.Done():
					return
				case <-published:
					inFlight--
				}
			}

			select {
			case <-ctx.Done():
				return
			default:
			}

			buf := make([]byte, opts.BlockSize)
			n, err := io.ReadFull(r, buf)
			if n > 0 {
				block := Block{Seq: seq, OriginalOffset: offset, Data: buf[:n]}
				select {
				case <-ctx.Done():
					return
				case jobs <- block:
				}
				inFlight++
				seq++
				offset += int64(n)
			}
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return
			}
			if err != nil {
				select {
				case <-ctx.Done():
				case results <- resultBlock{seq: seq, offset: offset, err: err}:
				}
				return
			}
		}
	}()

	publishedEntries, nextSeq, err := collectResults(ctx, cw, results, published, entries, startSeq)
	if err != nil {
		cancel()
		reader.Wait()
		return failureCheckpoint(opts, publishedEntries, nextSeq, err), err
	}
	return nil, nil
}

func failureCheckpoint(opts Options, entries []Entry, nextSeq int, cause error) *Checkpoint {
	dataEnd := int64(headerSize)
	var nextOffset int64
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		dataEnd = last.DataOffset + last.CompressedLength
		nextOffset = last.OriginalOffset + last.OriginalLength
	}
	reason := "interrupted"
	if cause != nil {
		reason = cause.Error()
	}
	return &Checkpoint{
		Version:    checkpointVersion,
		BlockSize:  opts.BlockSize,
		CodecIDs:   codecIDs(opts.Codecs),
		Entries:    entries,
		NextSeq:    nextSeq,
		NextOffset: nextOffset,
		DataEnd:    dataEnd,
		Reason:     reason,
	}
}

func collectResults(ctx context.Context, w *countingWriter, results <-chan resultBlock, published chan<- struct{}, entries []Entry, nextSeq int) ([]Entry, int, error) {
	pending := make(map[int]resultBlock)

	for {
		if next, ok := pending[nextSeq]; ok {
			delete(pending, nextSeq)
			if next.err != nil {
				return entries, nextSeq, next.err
			}

			dataOffset := w.n
			if err := writeFrame(w, next); err != nil {
				return entries, nextSeq, err
			}
			select {
			case published <- struct{}{}:
			case <-ctx.Done():
				return entries, nextSeq, ctx.Err()
			}
			entries = append(entries, Entry{
				Seq:              next.seq,
				OriginalOffset:   next.offset,
				OriginalLength:   int64(len(next.data)),
				DataOffset:       dataOffset,
				CompressedLength: int64(frameHeaderSize + len(next.payload)),
				Codec:            next.codec,
				Checksum:         next.checksum,
			})
			nextSeq++
			continue
		}

		select {
		case <-ctx.Done():
			return entries, nextSeq, ctx.Err()
		case result, ok := <-results:
			if !ok {
				indexOffset := w.n
				indexChecksum, err := writeIndex(w, entries)
				if err != nil {
					return entries, nextSeq, err
				}
				return entries, nextSeq, writeFooter(w, entries, indexOffset, indexChecksum)
			}
			if result.err != nil {
				return entries, nextSeq, result.err
			}
			if _, exists := pending[result.seq]; exists {
				return entries, nextSeq, ErrBadArchive
			}
			pending[result.seq] = result
		}
	}
}

func encodeBlock(block Block, opts Options) resultBlock {
	selected := opts.Selector(block, opts.Codecs)
	if selected == nil {
		selected = storeCodec{}
	}
	payload, err := selected.Encode(block.Data)
	if err != nil {
		return resultBlock{seq: block.Seq, offset: block.OriginalOffset, err: err}
	}

	actual := selected
	if len(payload) > len(block.Data) && selected.ID() != CodecStore {
		actual = codecByID(opts.Codecs, CodecStore)
		if actual == nil {
			actual = storeCodec{}
		}
		payload, err = actual.Encode(block.Data)
		if err != nil {
			return resultBlock{seq: block.Seq, offset: block.OriginalOffset, err: err}
		}
	}

	return resultBlock{
		seq:      block.Seq,
		offset:   block.OriginalOffset,
		data:     block.Data,
		codec:    actual.ID(),
		payload:  payload,
		checksum: crc32Checksum(block.Data),
	}
}
