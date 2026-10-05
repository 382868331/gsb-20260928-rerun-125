package datacompression

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

var (
	// ErrRepairConflict is reported when two repair candidates cover the
	// same block with content that does not agree. Content produced from
	// different input versions can never be assembled into one archive.
	ErrRepairConflict = errors.New("repair candidates conflict")
	// ErrRepairIncomplete is reported by a commit attempt when the
	// candidate still carries gaps: some requested blocks had no input.
	ErrRepairIncomplete = errors.New("repair candidate has gaps")
)

// Block repair item statuses as reported by RepairPlan.
const (
	// RepairReuse blocks are still intact in the original archive and are
	// reused byte for byte.
	RepairReuse = "reuse"
	// RepairRegenerate blocks are damaged but the matching original input
	// is available, so they must be encoded again.
	RepairRegenerate = "regenerate"
	// RepairMissingInput blocks are damaged and the original input does
	// not cover them, so nothing can be regenerated yet.
	RepairMissingInput = "missing_input"
	// RepairInputConflict blocks are damaged, but the supplied input does
	// not match the checksum recorded in the archive index: it comes from
	// a different input version and must not be spliced in.
	RepairInputConflict = "input_conflict"
)

// RepairRange is a half-open range of block sequence numbers [Start, End).
type RepairRange struct {
	Start int
	End   int
}

// RepairItem classifies a single block inside a repair request.
type RepairItem struct {
	Seq    int
	Status string
}

// RepairPlan lists, before anything is written, what can be reused, what
// must be regenerated, what is missing its original input and what was
// rejected because the input version does not match the archive.
type RepairPlan struct {
	BlockCount       int
	Ranges           []RepairRange
	Items            []RepairItem
	InputFingerprint uint64
}

func (p *RepairPlan) seqs(status string) []int {
	var out []int
	for _, item := range p.Items {
		if item.Status == status {
			out = append(out, item.Seq)
		}
	}
	return out
}

// Reusable returns the block sequences that are intact and reused as-is.
func (p *RepairPlan) Reusable() []int { return p.seqs(RepairReuse) }

// Regenerate returns the damaged block sequences whose original input is
// available.
func (p *RepairPlan) Regenerate() []int { return p.seqs(RepairRegenerate) }

// MissingInput returns damaged block sequences not covered by the input.
func (p *RepairPlan) MissingInput() []int { return p.seqs(RepairMissingInput) }

// InputConflicts returns block sequences whose supplied input does not
// match the archive's recorded checksums.
func (p *RepairPlan) InputConflicts() []int { return p.seqs(RepairInputConflict) }

// RepairFrame is one verified frame stored in a repair candidate.
type RepairFrame struct {
	Seq         int
	Offset      int64
	Length      int64
	Codec       uint16
	Checksum    uint32
	Regenerated bool
}

// RepairManifest is the durable description of a repair candidate. The
// candidate file itself holds the archive header followed by exactly the
// frames listed in Frames, in sequence order; Gaps lists requested blocks
// that could not be repaired yet.
type RepairManifest struct {
	Version             int
	BlockCount          int
	BlockSize           int
	SettingsFingerprint uint64
	InputFingerprint    uint64
	Ranges              []RepairRange
	Frames              []RepairFrame
	Gaps                []int
}

func (m *RepairManifest) normalize() {
	m.Ranges = mergeRanges(m.Ranges)
	sort.Ints(m.Gaps)
	sort.Slice(m.Frames, func(i, j int) bool { return m.Frames[i].Seq < m.Frames[j].Seq })
}

// RequestedSeqs is the union of every block sequence any task asked for.
func (m *RepairManifest) RequestedSeqs() []int {
	return rangeSeqs(m.Ranges, m.BlockCount)
}

// RepairedSeqs returns the sequences that actually carry a frame in the
// candidate.
func (m *RepairManifest) RepairedSeqs() []int {
	out := make([]int, 0, len(m.Frames))
	for _, frame := range m.Frames {
		out = append(out, frame.Seq)
	}
	return out
}

// Gap reports whether seq was requested but is still missing a frame.
func (m *RepairManifest) Gap(seq int) bool {
	i := sort.SearchInts(m.Gaps, seq)
	return i < len(m.Gaps) && m.Gaps[i] == seq
}

// SaveManifest atomically writes a repair manifest next to its candidate.
func SaveManifest(path string, m RepairManifest) error {
	m.normalize()
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadManifest reads a previously saved repair manifest.
func LoadManifest(path string) (RepairManifest, error) {
	var m RepairManifest
	data, err := os.ReadFile(path)
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, fmt.Errorf("%w: %v", ErrBadArchive, err)
	}
	if m.Version != formatVersion {
		return m, fmt.Errorf("%w: manifest version %d", ErrBadArchive, m.Version)
	}
	m.normalize()
	return m, nil
}

func mergeRanges(ranges []RepairRange) []RepairRange {
	if len(ranges) == 0 {
		return nil
	}
	sorted := append([]RepairRange(nil), ranges...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Start != sorted[j].Start {
			return sorted[i].Start < sorted[j].Start
		}
		return sorted[i].End < sorted[j].End
	})
	merged := []RepairRange{sorted[0]}
	for _, r := range sorted[1:] {
		last := &merged[len(merged)-1]
		if r.Start <= last.End {
			if r.End > last.End {
				last.End = r.End
			}
			continue
		}
		merged = append(merged, r)
	}
	return merged
}

func rangeSeqs(ranges []RepairRange, blockCount int) []int {
	seen := map[int]struct{}{}
	for _, r := range mergeRanges(ranges) {
		start := r.Start
		if start < 0 {
			start = 0
		}
		end := r.End
		if end > blockCount {
			end = blockCount
		}
		for seq := start; seq < end; seq++ {
			seen[seq] = struct{}{}
		}
	}
	out := make([]int, 0, len(seen))
	for seq := range seen {
		out = append(out, seq)
	}
	sort.Ints(out)
	return out
}

type fingerprint64 struct {
	h interface {
		Write([]byte) (int, error)
		Sum64() uint64
	}
}

func newFingerprint64() *fingerprint64 { return &fingerprint64{h: fnv.New64a()} }

func (f *fingerprint64) WriteUint64(v uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	f.h.Write(buf[:])
}

func (f *fingerprint64) Sum64() uint64 { return f.h.Sum64() }

// readRequestedBlocks reads input blocks from the beginning and keeps only
// the requested ones, returning an FNV-64a fingerprint over their CRCs in
// sequence order. A short read simply leaves the trailing blocks absent.
func readRequestedBlocks(r io.Reader, entries []Entry, requested map[int]bool, out map[int][]byte) (uint64, error) {
	fp := newFingerprint64()
	for _, entry := range entries {
		data := make([]byte, entry.OriginalLength)
		if _, err := io.ReadFull(r, data); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return 0, err
		}
		if requested[entry.Seq] {
			out[entry.Seq] = data
		}
		fp.WriteUint64(uint64(crc32Checksum(data)))
	}
	return fp.Sum64(), nil
}
