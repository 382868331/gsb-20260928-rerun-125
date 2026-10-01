package datacompression

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

// Checkpoint captures the confirmed progress of an interrupted compression
// run. Every Entry in Entries has been fully written to the output and
// fsynced by the caller's save hook before the next block is published, so
// the checkpoint never claims more than what is durable in the output.
type Checkpoint struct {
	Version      int      `json:"version"`
	BlockSize    int      `json:"block_size"`
	CodecIDs     []uint16 `json:"codec_ids"`
	Fingerprint  uint64   `json:"fingerprint"`
	Entries      []Entry  `json:"entries"`
	InputOffset  int64    `json:"input_offset"`
	OutputOffset int64    `json:"output_offset"`
	Interrupted  bool     `json:"interrupted"`
	Failure      string   `json:"failure,omitempty"`
}

func newCheckpoint(opts Options, entries []Entry, outputOffset int64, interrupted bool, cause error) Checkpoint {
	ids := make([]uint16, 0, len(opts.Codecs))
	for _, codec := range opts.Codecs {
		if codec != nil {
			ids = append(ids, codec.ID())
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	inputOffset := int64(0)
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		inputOffset = last.OriginalOffset + last.OriginalLength
	}

	cp := Checkpoint{
		Version:      formatVersion,
		BlockSize:    opts.BlockSize,
		CodecIDs:     ids,
		Fingerprint:  settingsFingerprint(opts),
		Entries:      append([]Entry(nil), entries...),
		InputOffset:  inputOffset,
		OutputOffset: outputOffset,
		Interrupted:  interrupted,
	}
	if cause != nil {
		cp.Failure = cause.Error()
	}
	return cp
}

// settingsFingerprint identifies the encoding settings a checkpoint is bound
// to. The selector function itself cannot be fingerprinted; callers that
// change selection logic between runs must treat old checkpoints as invalid.
func settingsFingerprint(opts Options) uint64 {
	ids := make([]uint16, 0, len(opts.Codecs))
	for _, codec := range opts.Codecs {
		if codec != nil {
			ids = append(ids, codec.ID())
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	h := fnv.New64a()
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(formatVersion))
	h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], uint64(opts.BlockSize))
	h.Write(buf[:])
	for _, id := range ids {
		binary.LittleEndian.PutUint16(buf[:2], id)
		h.Write(buf[:2])
	}
	return h.Sum64()
}

func (cp Checkpoint) validate(opts Options) error {
	if cp.Version != formatVersion {
		return fmt.Errorf("%w: checkpoint version %d", ErrIncompatibleState, cp.Version)
	}
	if cp.BlockSize != opts.BlockSize {
		return fmt.Errorf("%w: block size %d != %d", ErrIncompatibleState, cp.BlockSize, opts.BlockSize)
	}
	if cp.Fingerprint != settingsFingerprint(opts) {
		return fmt.Errorf("%w: encoding settings changed", ErrIncompatibleState)
	}
	expectedOffset := int64(0)
	for i, entry := range cp.Entries {
		if entry.Seq != i || entry.OriginalOffset != expectedOffset || entry.OriginalLength <= 0 {
			return fmt.Errorf("%w: corrupt checkpoint entry %d", ErrIncompatibleState, i)
		}
		expectedOffset += entry.OriginalLength
	}
	if cp.InputOffset != expectedOffset {
		return fmt.Errorf("%w: checkpoint input offset", ErrIncompatibleState)
	}
	return nil
}

// verifyPrefix re-reads the confirmed input prefix and checks every block
// against the checksum recorded in the checkpoint. Any change to the input
// content is reported as ErrIncompatibleState before any output is written.
func verifyPrefix(r io.Reader, entries []Entry) error {
	for _, entry := range entries {
		h := crc32.NewIEEE()
		if _, err := io.CopyN(h, r, entry.OriginalLength); err != nil {
			return fmt.Errorf("%w: re-reading confirmed prefix: %v", ErrIncompatibleState, err)
		}
		if h.Sum32() != entry.Checksum {
			return fmt.Errorf("%w: input changed at block %d", ErrIncompatibleState, entry.Seq)
		}
	}
	return nil
}

// SaveCheckpoint atomically persists a checkpoint next to the output.
func SaveCheckpoint(path string, cp Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadCheckpoint reads a previously saved checkpoint.
func LoadCheckpoint(path string) (Checkpoint, error) {
	var cp Checkpoint
	data, err := os.ReadFile(path)
	if err != nil {
		return cp, err
	}
	if err := json.Unmarshal(data, &cp); err != nil {
		return cp, fmt.Errorf("%w: %v", ErrIncompatibleState, err)
	}
	return cp, nil
}

func checkpointSaver(path string) func(Checkpoint) error {
	return func(cp Checkpoint) error {
		return SaveCheckpoint(path, cp)
	}
}

var errCheckpointNotFound = errors.New("checkpoint not found")

func loadCheckpointForResume(path string) (Checkpoint, error) {
	cp, err := LoadCheckpoint(path)
	if errors.Is(err, os.ErrNotExist) {
		return cp, errCheckpointNotFound
	}
	return cp, err
}
