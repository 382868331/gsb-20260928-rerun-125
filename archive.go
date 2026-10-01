package datacompression

import (
	"context"
	"errors"
	"io"
	"sync"
)

var (
	ErrBadArchive        = errors.New("invalid archive")
	ErrBadChecksum       = errors.New("archive checksum mismatch")
	ErrUnknownCodec      = errors.New("unknown codec")
	ErrBlockNotFound     = errors.New("block not found")
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrIncompatibleState = errors.New("incompatible checkpoint state")
	ErrIncomplete        = errors.New("archive incomplete")
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
	return runPipeline(ctx, w, r, opts, nil, nil)
}

// CompressWithCheckpoint behaves like Compress but reports a Checkpoint after
// every confirmed block publish and once more (marked interrupted) if the run
// fails, so a later Resume can continue from the confirmed prefix.
func CompressWithCheckpoint(ctx context.Context, w io.Writer, r io.Reader, opts Options, save func(Checkpoint) error) error {
	return runPipeline(ctx, w, r, opts, nil, save)
}

// Resume continues an interrupted run. The checkpoint is validated against
// opts (format version, block size and codec set must match), the confirmed
// input prefix is re-read from r and verified against the recorded checksums,
// and encoding continues from the first unconfirmed block. Already published
// frames are never rewritten: w must be positioned at cp.OutputOffset and r
// must yield the original input from the beginning. Any incompatibility
// rejects the resume before anything is written.
func Resume(ctx context.Context, w io.Writer, r io.Reader, opts Options, cp Checkpoint, save func(Checkpoint) error) error {
	return runPipeline(ctx, w, r, opts, &cp, save)
}

func runPipeline(ctx context.Context, w io.Writer, r io.Reader, opts Options, cp *Checkpoint, hook func(Checkpoint) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts = opts.withDefaults()
	if w == nil || r == nil || opts.BlockSize <= 0 || opts.Concurrency <= 0 || opts.Selector == nil {
		return ErrInvalidArgument
	}
	if codecByID(opts.Codecs, CodecStore) == nil {
		opts.Codecs = append(opts.Codecs, storeCodec{})
	}

	entries := make([]Entry, 0)
	startSeq := 0
	startOffset := int64(0)
	startWritten := int64(0)
	if cp != nil {
		if err := cp.validate(opts); err != nil {
			return err
		}
		if err := verifyPrefix(r, cp.Entries); err != nil {
			return err
		}
		entries = append(entries, cp.Entries...)
		startSeq = len(cp.Entries)
		startOffset = cp.InputOffset
		startWritten = cp.OutputOffset
	}

	cw := &countingWriter{w: w, n: startWritten}
	if cp == nil {
		if err := writeHeader(cw); err != nil {
			return err
		}
	}
	snapshot := func(interrupted bool, cause error) Checkpoint {
		return newCheckpoint(opts, entries, cw.n, interrupted, cause)
	}
	fail := func(cause error) error {
		if hook != nil {
			_ = hook(snapshot(true, cause))
		}
		return cause
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

	entries, err := collectResults(ctx, cw, entries, results, published, func(entry Entry) error {
		if hook == nil {
			return nil
		}
		return hook(snapshot(false, nil))
	})
	if err != nil {
		cancel()
		reader.Wait()
		return fail(err)
	}

	indexOffset := cw.n
	indexChecksum, err := writeIndex(cw, entries)
	if err != nil {
		return fail(err)
	}
	return writeFooter(cw, entries, indexOffset, indexChecksum)
}

func collectResults(ctx context.Context, w *countingWriter, entries []Entry, results <-chan resultBlock, published chan<- struct{}, onPublish func(Entry) error) ([]Entry, error) {
	pending := make(map[int]resultBlock)
	nextSeq := len(entries)

	for {
		if next, ok := pending[nextSeq]; ok {
			delete(pending, nextSeq)
			if next.err != nil {
				return entries, next.err
			}

			dataOffset := w.n
			if err := writeFrame(w, next); err != nil {
				return entries, err
			}
			entry := Entry{
				Seq:              next.seq,
				OriginalOffset:   next.offset,
				OriginalLength:   int64(len(next.data)),
				DataOffset:       dataOffset,
				CompressedLength: int64(frameHeaderSize + len(next.payload)),
				Codec:            next.codec,
				Checksum:         next.checksum,
			}
			entries = append(entries, entry)
			if onPublish != nil {
				if err := onPublish(entry); err != nil {
					return entries, err
				}
			}
			select {
			case published <- struct{}{}:
			case <-ctx.Done():
				return entries, ctx.Err()
			}
			nextSeq++
			continue
		}

		select {
		case <-ctx.Done():
			return entries, ctx.Err()
		case result, ok := <-results:
			if !ok {
				return entries, nil
			}
			if result.err != nil {
				return entries, result.err
			}
			if _, exists := pending[result.seq]; exists {
				return entries, ErrBadArchive
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
