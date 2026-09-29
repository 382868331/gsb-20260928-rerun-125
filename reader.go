package compresslab

import (
	"fmt"
	"hash/crc32"
	"io"
)

// Reader gives random access to the decoded content of a compresslab
// stream. It reads only the blocks overlapping the requested range.
type Reader struct {
	ra        io.ReaderAt
	blockSize uint32
	index     []BlockMeta
	size      int64 // total decoded size
}

// OpenReader validates the stream (header, footer, index checksum and
// layout) and returns a Reader. size is the total stream length in bytes.
func OpenReader(ra io.ReaderAt, size int64) (*Reader, error) {
	blockSize, index, err := readIndexAt(ra, size)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, m := range index {
		total += int64(m.RawLen)
	}
	return &Reader{ra: ra, blockSize: blockSize, index: index, size: total}, nil
}

// Size is the total decoded size in bytes.
func (r *Reader) Size() int64 { return r.size }

// Index returns a copy of the block index, including the codec actually
// used for each block.
func (r *Reader) Index() []BlockMeta {
	out := make([]BlockMeta, len(r.index))
	copy(out, r.index)
	return out
}

// DecodeBlock independently decodes and verifies block i. It is the
// read path used by ReadAt and is also exported so tests and tools can
// verify a stream block by block without going through Reader.
func DecodeBlock(ra io.ReaderAt, m BlockMeta) ([]byte, error) {
	c, ok := Lookup(m.CodecID)
	if !ok {
		return nil, fmt.Errorf("compresslab: unknown codec ID %d", m.CodecID)
	}
	buf := make([]byte, m.CompLen)
	if _, err := ra.ReadAt(buf, int64(m.Offset)); err != nil {
		return nil, fmt.Errorf("compresslab: reading block payload: %w", err)
	}
	raw, err := c.Decode(buf, int(m.RawLen))
	if err != nil {
		return nil, fmt.Errorf("compresslab: decoding block: %w", err)
	}
	if sum := crc32.ChecksumIEEE(raw); sum != m.CRC32 {
		return nil, fmt.Errorf("compresslab: block checksum mismatch")
	}
	return raw, nil
}

// ReadAt reads decoded bytes; it implements io.ReaderAt.
func (r *Reader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("compresslab: negative offset")
	}
	if off >= r.size {
		return 0, io.EOF
	}
	if int64(len(p)) > r.size-off {
		p = p[:r.size-off]
	}
	read := 0
	for len(p) > 0 {
		bi := int(off / int64(r.blockSize))
		if bi >= len(r.index) {
			return read, fmt.Errorf("compresslab: offset %d past last block", off)
		}
		raw, err := DecodeBlock(r.ra, r.index[bi])
		if err != nil {
			return read, fmt.Errorf("compresslab: block %d: %w", bi, err)
		}
		start := int(off % int64(r.blockSize))
		n := copy(p, raw[start:])
		p = p[n:]
		off += int64(n)
		read += n
	}
	return read, nil
}

// ReadSegment returns the decoded bytes in [off, off+length), clamped to
// the content size.
func (r *Reader) ReadSegment(off, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, fmt.Errorf("compresslab: negative offset or length")
	}
	if off >= r.size {
		return nil, io.EOF
	}
	if off+length > r.size {
		length = r.size - off
	}
	buf := make([]byte, length)
	if _, err := r.ReadAt(buf, off); err != nil {
		return nil, err
	}
	return buf, nil
}
