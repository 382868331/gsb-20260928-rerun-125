package datacompression

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
)

const checkpointVersion = 1

// Checkpoint captures the resumable state of an interrupted compression run.
// Entries holds only confirmed, fully published blocks; DataEnd is the offset
// in the partial output immediately after the last confirmed frame. Anything
// beyond DataEnd in the partial output is unconfirmed and must be discarded
// before resuming.
type Checkpoint struct {
	Version    int      `json:"version"`
	BlockSize  int      `json:"blockSize"`
	CodecIDs   []uint16 `json:"codecIDs"`
	Entries    []Entry  `json:"entries"`
	NextSeq    int      `json:"nextSeq"`
	NextOffset int64    `json:"nextOffset"`
	DataEnd    int64    `json:"dataEnd"`
	Reason     string   `json:"reason"`
}

func codecIDs(codecs []Codec) []uint16 {
	ids := make([]uint16, 0, len(codecs))
	seen := make(map[uint16]bool)
	for _, codec := range codecs {
		if codec == nil || seen[codec.ID()] {
			continue
		}
		seen[codec.ID()] = true
		ids = append(ids, codec.ID())
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// CompatibleWith reports whether a run with the given options may continue
// from this checkpoint. Block size and the codec set must match exactly;
// otherwise ErrIncompatibleCheckpoint is returned and the saved state must
// be left untouched.
func (c *Checkpoint) CompatibleWith(opts Options) error {
	opts = opts.withDefaults()
	if codecByID(opts.Codecs, CodecStore) == nil {
		opts.Codecs = append(opts.Codecs, storeCodec{})
	}
	if c.Version != checkpointVersion || c.BlockSize != opts.BlockSize {
		return ErrIncompatibleCheckpoint
	}
	want := codecIDs(opts.Codecs)
	if len(want) != len(c.CodecIDs) {
		return ErrIncompatibleCheckpoint
	}
	for i := range want {
		if want[i] != c.CodecIDs[i] {
			return ErrIncompatibleCheckpoint
		}
	}
	return nil
}

// SaveCheckpoint writes the checkpoint atomically to path.
func SaveCheckpoint(path string, c *Checkpoint) (err error) {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".checkpoint-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		if err != nil {
			os.Remove(tmpPath)
		}
	}()
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// LoadCheckpoint reads a previously saved checkpoint.
func LoadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCheckpointNotFound
		}
		return nil, err
	}
	var c Checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, err
	}
	if c.Version != checkpointVersion {
		return nil, ErrIncompatibleCheckpoint
	}
	return &c, nil
}
