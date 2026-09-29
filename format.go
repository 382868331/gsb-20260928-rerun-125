package datacompression

import (
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	archiveMagic    = "GCBK"
	formatVersion   = 1
	frameMagic      = 0x314d5246
	indexMagic      = "GIDX"
	footerMagic     = "GFOT"
	headerSize      = 8
	frameHeaderSize = 32
	indexHeaderSize = 8
	indexRecordSize = 52
	indexCRCSize    = 4
	footerSize      = 48
)

func crc32Checksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func writeHeader(w io.Writer) error {
	header := make([]byte, headerSize)
	copy(header[:4], archiveMagic)
	binary.LittleEndian.PutUint16(header[4:6], formatVersion)
	return writeFull(w, header)
}

func writeFrame(w io.Writer, block resultBlock) error {
	header := make([]byte, frameHeaderSize)
	binary.LittleEndian.PutUint32(header[0:4], frameMagic)
	binary.LittleEndian.PutUint16(header[4:6], block.codec)
	binary.LittleEndian.PutUint64(header[8:16], uint64(len(block.data)))
	binary.LittleEndian.PutUint64(header[16:24], uint64(len(block.payload)))
	binary.LittleEndian.PutUint32(header[24:28], block.checksum)
	if err := writeFull(w, header); err != nil {
		return err
	}
	return writeFull(w, block.payload)
}

func writeIndex(w io.Writer, entries []Entry) (uint32, error) {
	index := make([]byte, indexHeaderSize+len(entries)*indexRecordSize+indexCRCSize)
	copy(index[:4], indexMagic)
	binary.LittleEndian.PutUint32(index[4:8], uint32(len(entries)))

	for i, entry := range entries {
		record := index[indexHeaderSize+i*indexRecordSize:]
		putUint64(record[0:8], int64(entry.Seq))
		putUint64(record[8:16], entry.OriginalOffset)
		putUint64(record[16:24], entry.OriginalLength)
		putUint64(record[24:32], entry.DataOffset)
		putUint64(record[32:40], entry.CompressedLength)
		binary.LittleEndian.PutUint16(record[40:42], entry.Codec)
		binary.LittleEndian.PutUint32(record[44:48], entry.Checksum)
	}

	checksum := crc32.ChecksumIEEE(index[:len(index)-indexCRCSize])
	binary.LittleEndian.PutUint32(index[len(index)-indexCRCSize:], checksum)
	return checksum, writeFull(w, index)
}

func writeFooter(w io.Writer, entries []Entry, indexOffset int64, indexChecksum uint32) error {
	indexLength := int64(indexHeaderSize + len(entries)*indexRecordSize + indexCRCSize)
	footer := make([]byte, footerSize)
	copy(footer[:4], footerMagic)
	binary.LittleEndian.PutUint16(footer[4:6], formatVersion)
	putUint64(footer[8:16], indexOffset)
	putUint64(footer[16:24], indexLength)
	putUint64(footer[24:32], int64(len(entries)))
	binary.LittleEndian.PutUint32(footer[32:36], indexChecksum)
	binary.LittleEndian.PutUint32(footer[44:48], crc32.ChecksumIEEE(footer[:44]))
	return writeFull(w, footer)
}

func putUint64(dst []byte, value int64) {
	binary.LittleEndian.PutUint64(dst, uint64(value))
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
