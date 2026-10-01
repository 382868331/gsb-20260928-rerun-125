package datacompression

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Reader struct {
	r       io.ReaderAt
	closer  io.Closer
	size    int64
	codecs  map[uint16]Codec
	entries []Entry
}

func (r *Reader) Close() error {
	if r.closer != nil {
		return r.closer.Close()
	}
	return nil
}

func NewReader(r io.ReaderAt, size int64, codecs []Codec) (*Reader, error) {
	if r == nil || size < int64(headerSize+footerSize) {
		return nil, ErrBadArchive
	}

	header := make([]byte, headerSize)
	if _, err := r.ReadAt(header, 0); err != nil {
		return nil, err
	}
	if string(header[:4]) != archiveMagic {
		return nil, ErrBadArchive
	}
	if binary.LittleEndian.Uint16(header[4:6]) != formatVersion {
		return nil, ErrBadArchive
	}

	footerBytes := make([]byte, footerSize)
	if _, err := r.ReadAt(footerBytes, size-footerSize); err != nil {
		return nil, err
	}
	if string(footerBytes[:4]) != footerMagic {
		return nil, ErrBadArchive
	}
	if binary.LittleEndian.Uint16(footerBytes[4:6]) != formatVersion {
		return nil, ErrBadArchive
	}
	footerChecksum := binary.LittleEndian.Uint32(footerBytes[44:48])
	if crc32Checksum(footerBytes[:44]) != footerChecksum {
		return nil, fmt.Errorf("%w: footer checksum", ErrBadArchive)
	}

	indexOffset := int64(binary.LittleEndian.Uint64(footerBytes[8:16]))
	indexLength := int64(binary.LittleEndian.Uint64(footerBytes[16:24]))
	entryCount := int64(binary.LittleEndian.Uint64(footerBytes[24:32]))
	indexChecksum := binary.LittleEndian.Uint32(footerBytes[32:36])
	expectedLength := int64(indexHeaderSize + entryCount*indexRecordSize + indexCRCSize)
	if indexLength != expectedLength || indexOffset < headerSize || indexOffset+indexLength != size-footerSize {
		return nil, fmt.Errorf("%w: index location", ErrBadArchive)
	}

	index := make([]byte, indexLength)
	if _, err := r.ReadAt(index, indexOffset); err != nil {
		return nil, err
	}
	if string(index[:4]) != indexMagic {
		return nil, ErrBadArchive
	}
	if int64(binary.LittleEndian.Uint32(index[4:8])) != entryCount {
		return nil, fmt.Errorf("%w: index count", ErrBadArchive)
	}
	if crc32Checksum(index[:indexLength-indexCRCSize]) != indexChecksum ||
		binary.LittleEndian.Uint32(index[indexLength-indexCRCSize:]) != indexChecksum {
		return nil, fmt.Errorf("%w: index checksum", ErrBadArchive)
	}

	entries := make([]Entry, entryCount)
	previous := endOfData{}
	for i := range entries {
		record := index[indexHeaderSize+i*indexRecordSize:]
		entry := Entry{
			Seq:              int(int64(binary.LittleEndian.Uint64(record[0:8]))),
			OriginalOffset:   int64(binary.LittleEndian.Uint64(record[8:16])),
			OriginalLength:   int64(binary.LittleEndian.Uint64(record[16:24])),
			DataOffset:       int64(binary.LittleEndian.Uint64(record[24:32])),
			CompressedLength: int64(binary.LittleEndian.Uint64(record[32:40])),
			Codec:            binary.LittleEndian.Uint16(record[40:42]),
			Checksum:         binary.LittleEndian.Uint32(record[44:48]),
		}
		if entry.Seq != i || entry.OriginalLength < 0 || entry.CompressedLength < frameHeaderSize ||
			entry.DataOffset < headerSize || entry.DataOffset+entry.CompressedLength > indexOffset {
			return nil, fmt.Errorf("%w: index entry %d", ErrBadArchive, i)
		}
		if i == 0 {
			if entry.OriginalOffset != 0 {
				return nil, fmt.Errorf("%w: original offset", ErrBadArchive)
			}
		} else if entry.OriginalOffset != previous.offset || entry.DataOffset != previous.dataEnd {
			return nil, fmt.Errorf("%w: non-contiguous index", ErrBadArchive)
		}
		previous.offset = entry.OriginalOffset + entry.OriginalLength
		previous.dataEnd = entry.DataOffset + entry.CompressedLength
		entries[i] = entry
	}

	codecMap := make(map[uint16]Codec)
	for _, codec := range append(DefaultCodecs(), codecs...) {
		if codec != nil {
			codecMap[codec.ID()] = codec
		}
	}

	return &Reader{r: r, size: size, codecs: codecMap, entries: entries}, nil
}

type endOfData struct {
	offset  int64
	dataEnd int64
}

func NewReaderFromBytes(data []byte, codecs []Codec) (*Reader, error) {
	return NewReader(bytes.NewReader(data), int64(len(data)), codecs)
}

func (r *Reader) Entries() []Entry {
	entries := make([]Entry, len(r.entries))
	copy(entries, r.entries)
	return entries
}

func (r *Reader) ReadBlock(seq int) ([]byte, error) {
	if seq < 0 || seq >= len(r.entries) {
		return nil, ErrBlockNotFound
	}
	return readBlockAt(r.r, r.codecs, r.entries[seq])
}
