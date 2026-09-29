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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	opts = opts.withDefaults()
	if w == nil || r == nil || opts.BlockSize <= 0 || opts.Concurrency <= 0 || opts.Selector == nil {
		return ErrInvalidArgument
	}
	if codecByID(opts.Codecs, CodecStore) == nil {
		opts.Codecs = append(opts.Codecs, storeCodec{})
	}

	cw := &countingWriter{w: w}
	if err := writeHeader(cw); err != nil {
		return err
	}

	jobs := make(chan Block, opts.Concurrency)
	results := make(chan resultBlock, opts.Concurrency)
	var workers sync.WaitGroup
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

	go func() {
		defer close(jobs)
		seq := 0
		offset := int64(0)
		for {
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

	return collectResults(ctx, cw, results)
}

func collectResults(ctx context.Context, w *countingWriter, results <-chan resultBlock) error {
	entries := make([]Entry, 0)
	pending := make(map[int]resultBlock)
	nextSeq := 0

	for {
		if next, ok := pending[nextSeq]; ok {
			delete(pending, nextSeq)
			if next.err != nil {
				return next.err
			}

			dataOffset := w.n
			if err := writeFrame(w, next); err != nil {
				return err
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
			return ctx.Err()
		case result, ok := <-results:
			if !ok {
				indexOffset := w.n
				indexChecksum, err := writeIndex(w, entries)
				if err != nil {
					return err
				}
				return writeFooter(w, entries, indexOffset, indexChecksum)
			}
			if result.err != nil {
				return result.err
			}
			if _, exists := pending[result.seq]; exists {
				return ErrBadArchive
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
