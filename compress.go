package compresslab

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"runtime"
	"sync"
)

// Options controls compression.
type Options struct {
	// BlockSize is the raw block size in bytes. Default 256 KiB.
	BlockSize int
	// Workers is the number of parallel encoding workers. Default
	// runtime.NumCPU().
	Workers int
	// MaxInFlight bounds how many blocks may be in flight (queued,
	// encoding, or encoded-but-unpublished) at once. A slow block
	// therefore applies backpressure instead of letting finished later
	// results pile up without limit. Default 2*Workers.
	MaxInFlight int
}

func (o *Options) defaults() {
	if o.BlockSize <= 0 {
		o.BlockSize = 256 << 10
	}
	if o.Workers <= 0 {
		o.Workers = runtime.NumCPU()
	}
	if o.MaxInFlight <= 0 {
		o.MaxInFlight = 2 * o.Workers
	}
}

// pickCodec encodes src with every registered candidate and returns the
// smallest result. If nothing beats the raw size the block is stored
// verbatim with the store codec, so the index always reflects what was
// actually used.
func pickCodec(src []byte) (BlockMeta, []byte, error) {
	best := BlockMeta{CodecID: codecIDStore, RawLen: uint32(len(src)), CRC32: crc32.ChecksumIEEE(src)}
	bestData, err := storeCodec{}.Encode(src)
	if err != nil {
		return best, nil, err
	}
	for _, c := range candidates() {
		enc, err := c.Encode(src)
		if err != nil {
			continue // a codec that fails on a block simply does not compete
		}
		if len(enc) < len(bestData) {
			best.CodecID = c.ID()
			bestData = enc
		}
	}
	best.CompLen = uint32(len(bestData))
	return best, bestData, nil
}

type job struct {
	seq  int
	data []byte
}

type result struct {
	seq  int
	meta BlockMeta
	data []byte
	err  error
}

// Compress reads all of r and writes a compresslab stream to w. Blocks
// are encoded in parallel but published strictly in input order, and the
// number of in-flight blocks is bounded so a slow block cannot let
// finished later blocks pile up without limit. Cancelling ctx aborts the
// write; whatever partial bytes reached w are not a valid stream because
// the index and footer are only written on success.
func Compress(ctx context.Context, r io.Reader, w io.Writer, opts Options) error {
	opts.defaults()
	if err := writeHeader(w, uint32(opts.BlockSize)); err != nil {
		return err
	}

	jobs := make(chan job, opts.MaxInFlight)
	results := make(chan result, opts.MaxInFlight)
	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				meta, data, err := pickCodec(j.data)
				select {
				case results <- result{seq: j.seq, meta: meta, data: data, err: err}:
				case <-done:
					return
				}
			}
		}()
	}
	go func() { wg.Wait(); close(results) }()

	// Producer: split input into blocks. The bounded jobs channel means
	// a slow block applies backpressure to reading.
	producerErr := make(chan error, 1)
	go func() {
		defer close(jobs)
		seq := 0
		for {
			buf := make([]byte, opts.BlockSize)
			n, err := io.ReadFull(r, buf)
			if n > 0 {
				select {
				case jobs <- job{seq: seq, data: buf[:n]}:
					seq++
				case <-ctx.Done():
					producerErr <- ctx.Err()
					return
				}
			}
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				producerErr <- nil
				return
			}
			if err != nil {
				producerErr <- err
				return
			}
		}
	}()

	// Publisher: emit results strictly in sequence order. The reorder
	// buffer stays bounded by MaxInFlight because the producer can run
	// at most that many blocks ahead.
	pending := map[int]result{}
	next := 0
	var index []BlockMeta
	offset := uint64(headerSize)
	open := true
	for open || len(pending) > 0 {
		if res, ok := pending[next]; ok {
			delete(pending, next)
			if res.err != nil {
				return res.err
			}
			if _, err := w.Write(res.data); err != nil {
				return err
			}
			res.meta.Offset = offset
			offset += uint64(res.meta.CompLen)
			index = append(index, res.meta)
			next++
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res, ok := <-results:
			if !ok {
				open = false
				continue
			}
			pending[res.seq] = res
		}
	}
	if err := <-producerErr; err != nil {
		return err
	}

	indexBuf := encodeIndex(index)
	if _, err := w.Write(indexBuf); err != nil {
		return err
	}
	return writeFooter(w, offset, uint32(len(index)), crc32.ChecksumIEEE(indexBuf))
}

// CompressFile compresses r into the file at path atomically: the stream
// is written to a temporary file in the same directory and renamed into
// place only on success. Cancelling ctx or any error removes the
// temporary file, so a cancelled run never leaves a complete-looking
// artifact behind.
func CompressFile(ctx context.Context, r io.Reader, path string, opts Options) (err error) {
	dir := dirOf(path)
	tmp, err := os.CreateTemp(dir, ".compresslab-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	if err = Compress(ctx, r, tmp, opts); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("compresslab: publishing %s: %w", path, err)
	}
	return nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return "."
}
