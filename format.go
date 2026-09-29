package compresslab

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

// Stream layout:
//
//	header:  magic(8) | blockSize u32
//	blocks:  concatenated encoded payloads
//	index:   per block: codecID u8 | rawLen u32 | compLen u32 | crc32 u32
//	footer:  indexOffset u64 | blockCount u32 | indexCRC u32 | magic(8)
//
// The footer is fixed size and located at the very end so a reader can
// find the index without scanning.
var (
	headerMagic = []byte("CLAB0001")
	footerMagic = []byte("CLABEND1")
)

const (
	headerSize   = 12
	indexEntSize = 13
	footerSize   = 24
)

// BlockMeta describes one block in the index.
type BlockMeta struct {
	CodecID uint8
	RawLen  uint32
	CompLen uint32
	CRC32   uint32 // CRC-32 of the decoded block
	// Offset is the payload position in the stream, filled in when the
	// index is attached to a stream.
	Offset uint64
}

func writeHeader(w io.Writer, blockSize uint32) error {
	var h [headerSize]byte
	copy(h[:8], headerMagic)
	binary.BigEndian.PutUint32(h[8:], blockSize)
	_, err := w.Write(h[:])
	return err
}

func encodeIndex(entries []BlockMeta) []byte {
	buf := make([]byte, 0, len(entries)*indexEntSize)
	for _, e := range entries {
		var b [indexEntSize]byte
		b[0] = e.CodecID
		binary.BigEndian.PutUint32(b[1:5], e.RawLen)
		binary.BigEndian.PutUint32(b[5:9], e.CompLen)
		binary.BigEndian.PutUint32(b[9:13], e.CRC32)
		buf = append(buf, b[:]...)
	}
	return buf
}

func decodeIndex(buf []byte, count int) ([]BlockMeta, error) {
	if len(buf) != count*indexEntSize {
		return nil, fmt.Errorf("compresslab: index has %d bytes, want %d", len(buf), count*indexEntSize)
	}
	entries := make([]BlockMeta, count)
	for i := range entries {
		b := buf[i*indexEntSize:]
		entries[i] = BlockMeta{
			CodecID: b[0],
			RawLen:  binary.BigEndian.Uint32(b[1:5]),
			CompLen: binary.BigEndian.Uint32(b[5:9]),
			CRC32:   binary.BigEndian.Uint32(b[9:13]),
		}
	}
	return entries, nil
}

func writeFooter(w io.Writer, indexOffset uint64, blockCount uint32, indexCRC uint32) error {
	var f [footerSize]byte
	binary.BigEndian.PutUint64(f[0:8], indexOffset)
	binary.BigEndian.PutUint32(f[8:12], blockCount)
	binary.BigEndian.PutUint32(f[12:16], indexCRC)
	copy(f[16:24], footerMagic)
	_, err := w.Write(f[:])
	return err
}

type footer struct {
	IndexOffset uint64
	BlockCount  uint32
	IndexCRC    uint32
}

// readFooterAt reads and validates the trailing footer.
func readFooterAt(ra io.ReaderAt, size int64) (footer, error) {
	if size < headerSize+footerSize {
		return footer{}, fmt.Errorf("compresslab: stream too small (%d bytes)", size)
	}
	var f [footerSize]byte
	if _, err := ra.ReadAt(f[:], size-footerSize); err != nil {
		return footer{}, fmt.Errorf("compresslab: reading footer: %w", err)
	}
	if string(f[16:24]) != string(footerMagic) {
		return footer{}, fmt.Errorf("compresslab: bad footer magic (truncated or not a compresslab stream)")
	}
	return footer{
		IndexOffset: binary.BigEndian.Uint64(f[0:8]),
		BlockCount:  binary.BigEndian.Uint32(f[8:12]),
		IndexCRC:    binary.BigEndian.Uint32(f[12:16]),
	}, nil
}

// readIndexAt loads and validates the full index, filling in offsets.
func readIndexAt(ra io.ReaderAt, size int64) (uint32, []BlockMeta, error) {
	var h [headerSize]byte
	if _, err := ra.ReadAt(h[:], 0); err != nil {
		return 0, nil, fmt.Errorf("compresslab: reading header: %w", err)
	}
	if string(h[:8]) != string(headerMagic) {
		return 0, nil, fmt.Errorf("compresslab: bad header magic")
	}
	blockSize := binary.BigEndian.Uint32(h[8:12])
	if blockSize == 0 {
		return 0, nil, fmt.Errorf("compresslab: zero block size in header")
	}

	f, err := readFooterAt(ra, size)
	if err != nil {
		return 0, nil, err
	}
	indexLen := uint64(f.BlockCount) * indexEntSize
	if f.IndexOffset < headerSize || f.IndexOffset+indexLen+footerSize != uint64(size) {
		return 0, nil, fmt.Errorf("compresslab: index offset/length inconsistent with stream size")
	}
	buf := make([]byte, indexLen)
	if _, err := ra.ReadAt(buf, int64(f.IndexOffset)); err != nil {
		return 0, nil, fmt.Errorf("compresslab: reading index: %w", err)
	}
	if sum := crc32.ChecksumIEEE(buf); sum != f.IndexCRC {
		return 0, nil, fmt.Errorf("compresslab: index checksum mismatch (corrupt index)")
	}
	entries, err := decodeIndex(buf, int(f.BlockCount))
	if err != nil {
		return 0, nil, err
	}
	off := uint64(headerSize)
	for i := range entries {
		entries[i].Offset = off
		next := off + uint64(entries[i].CompLen)
		if next > f.IndexOffset {
			return 0, nil, fmt.Errorf("compresslab: block %d overruns index", i)
		}
		off = next
	}
	if off != f.IndexOffset {
		return 0, nil, fmt.Errorf("compresslab: block payloads do not reach the index")
	}
	return blockSize, entries, nil
}
