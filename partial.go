package datacompression

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PartialReader provides verifiable read access to an archive whose output
// may not have finished. Frames are scanned sequentially from the header and
// every block is decoded and checksum-verified before it is reported as
// complete. A truncated or corrupt tail is never treated as complete: it is
// exposed through TailError instead.
type PartialReader struct {
	r        io.ReaderAt
	size     int64
	codecs   map[uint16]Codec
	entries  []Entry
	complete bool
	tailErr  error
}

// NewPartialReader opens possibly unfinished output. If a valid footer and
// index are present the archive is complete; otherwise frames are scanned
// and verified one by one until the confirmed prefix ends.
func NewPartialReader(r io.ReaderAt, size int64, codecs []Codec) (*PartialReader, error) {
	if r == nil || size < int64(headerSize) {
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
	p := &PartialReader{r: r, size: size, codecs: codecMap}

	offset := int64(headerSize)
	originalOffset := int64(0)
	scanErr := error(nil)
	for {
		if offset+4 > p.size {
			scanErr = fmt.Errorf("%w: frame header at %d truncated", ErrIncomplete, offset)
			break
		}
		magic := make([]byte, 4)
		if _, err := p.r.ReadAt(magic, offset); err != nil {
			scanErr = fmt.Errorf("%w: frame header at %d: %v", ErrIncomplete, offset, err)
			break
		}
		if binary.LittleEndian.Uint32(magic) != frameMagic {
			// End of the frame stream: a complete archive keeps its
			// index and footer here.
			break
		}
		entry, next, err := p.scanFrame(offset, originalOffset)
		if err != nil {
			scanErr = err
			break
		}
		p.entries = append(p.entries, entry)
		offset = next
		originalOffset += entry.OriginalLength
	}
	if scanErr != nil {
		p.tailErr = scanErr
		return p, nil
	}
	if full, err := NewReader(r, size, codecs); err == nil && entriesEqual(full.Entries(), p.entries) {
		p.complete = true
		return p, nil
	}
	p.tailErr = fmt.Errorf("%w: index and footer missing or invalid", ErrIncomplete)
	return p, nil
}

func entriesEqual(a, b []Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// scanFrame verifies the frame at offset and returns its entry plus the
// offset of the next frame. Any truncation or corruption is an error and the
// frame is excluded from the confirmed set.
func (p *PartialReader) scanFrame(offset, originalOffset int64) (Entry, int64, error) {
	var entry Entry
	header := make([]byte, frameHeaderSize)
	if _, err := readFullAt(p.r, header, offset); err != nil {
		return entry, 0, fmt.Errorf("%w: frame header at %d: %v", ErrIncomplete, offset, err)
	}
	if binary.LittleEndian.Uint32(header[0:4]) != frameMagic {
		return entry, 0, fmt.Errorf("%w: frame magic at %d", ErrBadArchive, offset)
	}
	codecID := binary.LittleEndian.Uint16(header[4:6])
	originalLength := int64(binary.LittleEndian.Uint64(header[8:16]))
	compressedLength := int64(binary.LittleEndian.Uint64(header[16:24]))
	checksum := binary.LittleEndian.Uint32(header[24:28])
	if originalLength < 0 || compressedLength < 0 {
		return entry, 0, fmt.Errorf("%w: frame lengths at %d", ErrBadArchive, offset)
	}
	end := offset + int64(frameHeaderSize) + compressedLength
	if end > p.size {
		return entry, 0, fmt.Errorf("%w: frame payload at %d truncated", ErrIncomplete, offset)
	}
	payload := make([]byte, compressedLength)
	if _, err := readFullAt(p.r, payload, offset+int64(frameHeaderSize)); err != nil {
		return entry, 0, fmt.Errorf("%w: frame payload at %d: %v", ErrIncomplete, offset, err)
	}
	codec := p.codecs[codecID]
	if codec == nil {
		return entry, 0, fmt.Errorf("%w: codec %d", ErrUnknownCodec, codecID)
	}
	decoded, err := codec.Decode(payload)
	if err != nil {
		return entry, 0, fmt.Errorf("%w: frame at %d: %v", ErrBadArchive, offset, err)
	}
	if int64(len(decoded)) != originalLength || crc32Checksum(decoded) != checksum {
		return entry, 0, fmt.Errorf("%w: frame at %d", ErrBadChecksum, offset)
	}

	entry = Entry{
		Seq:              len(p.entries),
		OriginalOffset:   originalOffset,
		OriginalLength:   originalLength,
		DataOffset:       offset,
		CompressedLength: int64(frameHeaderSize) + compressedLength,
		Codec:            codecID,
		Checksum:         checksum,
	}
	return entry, end, nil
}

func readFullAt(r io.ReaderAt, buf []byte, offset int64) (int, error) {
	n, err := r.ReadAt(buf, offset)
	if err == nil && n < len(buf) {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

// Complete reports whether the archive carries a valid index and footer.
func (p *PartialReader) Complete() bool { return p.complete }

// TailError explains why the output is incomplete: the tail is truncated
// (ErrIncomplete) or corrupt (ErrBadArchive / ErrBadChecksum). It is nil for
// complete archives.
func (p *PartialReader) TailError() error { return p.tailErr }

// Entries lists only the blocks confirmed complete and verified.
func (p *PartialReader) Entries() []Entry {
	entries := make([]Entry, len(p.entries))
	copy(entries, p.entries)
	return entries
}

// ConfirmedOffset is the number of original input bytes covered by confirmed
// blocks; anything beyond it was never published.
func (p *PartialReader) ConfirmedOffset() int64 {
	if len(p.entries) == 0 {
		return 0
	}
	last := p.entries[len(p.entries)-1]
	return last.OriginalOffset + last.OriginalLength
}

// ReadBlock decodes a confirmed block. Blocks past the confirmed prefix
// report ErrBlockNotFound.
func (p *PartialReader) ReadBlock(seq int) ([]byte, error) {
	if seq < 0 || seq >= len(p.entries) {
		return nil, ErrBlockNotFound
	}
	return readBlockAt(p.r, p.codecs, p.entries[seq])
}

func readBlockAt(r io.ReaderAt, codecs map[uint16]Codec, entry Entry) ([]byte, error) {
	frame := make([]byte, entry.CompressedLength)
	if _, err := r.ReadAt(frame, entry.DataOffset); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(frame[:4]) != frameMagic {
		return nil, fmt.Errorf("%w: frame magic", ErrBadArchive)
	}
	if binary.LittleEndian.Uint16(frame[4:6]) != entry.Codec {
		return nil, fmt.Errorf("%w: codec mismatch", ErrBadArchive)
	}
	originalLength := int64(binary.LittleEndian.Uint64(frame[8:16]))
	compressedLength := int64(binary.LittleEndian.Uint64(frame[16:24]))
	checksum := binary.LittleEndian.Uint32(frame[24:28])
	if originalLength != entry.OriginalLength || compressedLength != entry.CompressedLength-frameHeaderSize || checksum != entry.Checksum {
		return nil, fmt.Errorf("%w: frame metadata", ErrBadArchive)
	}

	codec := codecs[entry.Codec]
	if codec == nil {
		return nil, fmt.Errorf("%w: codec %d", ErrUnknownCodec, entry.Codec)
	}
	decoded, err := codec.Decode(frame[frameHeaderSize:])
	if err != nil {
		return nil, err
	}
	if int64(len(decoded)) != originalLength || crc32Checksum(decoded) != checksum {
		return nil, ErrBadChecksum
	}
	return decoded, nil
}
