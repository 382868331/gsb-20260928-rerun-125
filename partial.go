package datacompression

import (
	"encoding/binary"
	"io"
	"os"
)

// PartialReader provides verifiable read access to an incomplete archive,
// such as the partial output preserved after an interrupted run. It confirms
// exactly which blocks are fully published and decodable; a truncated or
// corrupt tail is never treated as complete.
type PartialReader struct {
	r         io.ReaderAt
	closer    io.Closer
	size      int64
	codecs    map[uint16]Codec
	entries   []Entry
	complete  bool
	stoppedAt int64
}

// NewPartialReader opens any archive prefix for inspection. If the data is a
// complete, valid archive the reader is marked complete and all blocks are
// available. Otherwise frames are scanned and verified one by one, and only
// the confirmed prefix is exposed.
func NewPartialReader(r io.ReaderAt, size int64, codecs []Codec) (*PartialReader, error) {
	if r == nil || size < headerSize {
		return nil, ErrBadArchive
	}
	header := make([]byte, headerSize)
	if _, err := r.ReadAt(header, 0); err != nil {
		return nil, err
	}
	if string(header[:4]) != archiveMagic ||
		binary.LittleEndian.Uint16(header[4:6]) != formatVersion {
		return nil, ErrBadArchive
	}

	codecMap := make(map[uint16]Codec)
	for _, codec := range append(DefaultCodecs(), codecs...) {
		if codec != nil {
			codecMap[codec.ID()] = codec
		}
	}

	if full, err := NewReader(r, size, codecs); err == nil {
		entries := full.Entries()
		if len(entries) > 0 {
			// A valid index alone does not prove the tail frame is intact;
			// verify the final block before reporting completion.
			if _, err := decodeFrame(r, codecMap, entries[len(entries)-1]); err != nil {
				return scanPartial(r, size, codecMap), nil
			}
		}
		return &PartialReader{
			r:         r,
			size:      size,
			codecs:    codecMap,
			entries:   entries,
			complete:  true,
			stoppedAt: size,
		}, nil
	}

	return scanPartial(r, size, codecMap), nil
}

func scanPartial(r io.ReaderAt, size int64, codecMap map[uint16]Codec) *PartialReader {
	p := &PartialReader{r: r, size: size, codecs: codecMap}
	offset := int64(headerSize)
	var originalOffset int64
	for {
		p.stoppedAt = offset
		remaining := size - offset
		if remaining < frameHeaderSize {
			break
		}
		header := make([]byte, frameHeaderSize)
		if _, err := r.ReadAt(header, offset); err != nil {
			break
		}
		if binary.LittleEndian.Uint32(header[0:4]) != frameMagic {
			break
		}
		originalLength := int64(binary.LittleEndian.Uint64(header[8:16]))
		payloadLength := int64(binary.LittleEndian.Uint64(header[16:24]))
		if originalLength < 0 || payloadLength < 0 || payloadLength > remaining-frameHeaderSize {
			break
		}
		entry := Entry{
			Seq:              len(p.entries),
			OriginalOffset:   originalOffset,
			OriginalLength:   originalLength,
			DataOffset:       offset,
			CompressedLength: frameHeaderSize + payloadLength,
			Codec:            binary.LittleEndian.Uint16(header[4:6]),
			Checksum:         binary.LittleEndian.Uint32(header[24:28]),
		}
		// Fully decode and verify the block before confirming it; a frame
		// that cannot be verified marks the start of the unconfirmed tail.
		if _, err := decodeFrame(r, codecMap, entry); err != nil {
			break
		}
		p.entries = append(p.entries, entry)
		originalOffset += originalLength
		offset += entry.CompressedLength
	}
	return p
}

// OpenPartialReader opens a partial or complete archive file for inspection.
func OpenPartialReader(path string, codecs []Codec) (*PartialReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	reader, err := NewPartialReader(file, info.Size(), codecs)
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	reader.closer = file
	return reader, nil
}

func (p *PartialReader) Close() error {
	if p.closer != nil {
		return p.closer.Close()
	}
	return nil
}

// Complete reports whether the data forms a full, valid archive including
// index and footer.
func (p *PartialReader) Complete() bool { return p.complete }

// CompleteBlocks returns the confirmed, fully published blocks in order.
func (p *PartialReader) CompleteBlocks() []Entry {
	entries := make([]Entry, len(p.entries))
	copy(entries, p.entries)
	return entries
}

// NextSeq returns the sequence number of the first block that is not yet
// published; blocks at or after this index are unavailable.
func (p *PartialReader) NextSeq() int { return len(p.entries) }

// StoppedAt returns the offset where verification stopped, i.e. the start of
// the unconfirmed or corrupt tail.
func (p *PartialReader) StoppedAt() int64 { return p.stoppedAt }

// ReadBlock decodes a confirmed block. Blocks beyond the confirmed prefix
// return ErrBlockNotFound.
func (p *PartialReader) ReadBlock(seq int) ([]byte, error) {
	if seq < 0 || seq >= len(p.entries) {
		return nil, ErrBlockNotFound
	}
	return decodeFrame(p.r, p.codecs, p.entries[seq])
}
