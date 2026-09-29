// Package compresslab implements a block-based compression stream with
// pluggable codecs, per-block strategy selection, an index for random
// access reads, and ordered parallel compression.
package compresslab

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"sort"
	"sync"
)

// Codec encodes and decodes a single data block. Implementations must be
// safe for concurrent use.
type Codec interface {
	// ID is the stable on-disk identifier recorded in the index. Once
	// published it must never be reused by a different codec.
	ID() uint8
	Name() string
	// Encode transforms one raw block.
	Encode(src []byte) ([]byte, error)
	// Decode reverses Encode; rawLen is the expected decoded size.
	Decode(src []byte, rawLen int) ([]byte, error)
}

var (
	registryMu sync.RWMutex
	registry   = map[uint8]Codec{}
)

// Register adds a codec to the global registry. It panics on a duplicate
// ID, which keeps misconfiguration loud at init time.
func Register(c Codec) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if c.ID() == codecIDStore {
		panic("compresslab: codec ID 0 is reserved for store")
	}
	if _, exists := registry[c.ID()]; exists {
		panic(fmt.Sprintf("compresslab: duplicate codec ID %d (%s)", c.ID(), c.Name()))
	}
	registry[c.ID()] = c
}

// Lookup returns the codec for an on-disk ID.
func Lookup(id uint8) (Codec, bool) {
	if id == codecIDStore {
		return storeCodec{}, true
	}
	registryMu.RLock()
	defer registryMu.RUnlock()
	c, ok := registry[id]
	return c, ok
}

// candidates returns all registered compression codecs in deterministic
// ID order. The store codec is implicit and never a candidate.
func candidates() []Codec {
	registryMu.RLock()
	defer registryMu.RUnlock()
	ids := make([]int, 0, len(registry))
	for id := range registry {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	out := make([]Codec, 0, len(ids))
	for _, id := range ids {
		out = append(out, registry[uint8(id)])
	}
	return out
}

const codecIDStore uint8 = 0

// storeCodec keeps a block uncompressed. It is built in and always
// available so a stream is never larger than its input by more than the
// index overhead.
type storeCodec struct{}

func (storeCodec) ID() uint8    { return codecIDStore }
func (storeCodec) Name() string { return "store" }
func (storeCodec) Encode(src []byte) ([]byte, error) {
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}
func (storeCodec) Decode(src []byte, rawLen int) ([]byte, error) {
	if len(src) != rawLen {
		return nil, fmt.Errorf("compresslab: stored block has %d bytes, want %d", len(src), rawLen)
	}
	out := make([]byte, len(src))
	copy(out, src)
	return out, nil
}

// FlateCodec wraps compress/flate for general-purpose blocks.
type FlateCodec struct {
	// CodecID defaults to 1 when zero.
	CodecID uint8
	Level   int
}

func (f FlateCodec) ID() uint8 {
	if f.CodecID == 0 {
		return 1
	}
	return f.CodecID
}
func (f FlateCodec) Name() string { return "flate" }

func (f FlateCodec) Encode(src []byte) ([]byte, error) {
	level := f.Level
	if level == 0 {
		level = flate.DefaultCompression
	}
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, level)
	if err != nil {
		return nil, err
	}
	if _, err := w.Write(src); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f FlateCodec) Decode(src []byte, rawLen int) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(out) != rawLen {
		return nil, fmt.Errorf("compresslab: flate block decoded to %d bytes, want %d", len(out), rawLen)
	}
	return out, nil
}

// RLECodec is a simple run-length codec that wins on highly repetitive
// blocks where flate's header overhead dominates.
type RLECodec struct {
	// CodecID defaults to 2 when zero.
	CodecID uint8
}

func (r RLECodec) ID() uint8 {
	if r.CodecID == 0 {
		return 2
	}
	return r.CodecID
}
func (r RLECodec) Name() string { return "rle" }

// Encoding: pairs of (count, value); the pair 0xFF 0x00 introduces a
// literal run: 0xFF 0x00 lenHi lenLo <literal bytes>.
func (r RLECodec) Encode(src []byte) ([]byte, error) {
	var out []byte
	i := 0
	for i < len(src) {
		run := 1
		for i+run < len(src) && src[i+run] == src[i] && run < 255 {
			run++
		}
		if run >= 3 {
			out = append(out, byte(run), src[i])
			i += run
			continue
		}
		start := i
		i += run
		for i < len(src) && i-start < 0xFFFF {
			look := 1
			for i+look < len(src) && src[i+look] == src[i] && look < 255 {
				look++
			}
			if look >= 3 {
				break
			}
			i += look
		}
		lit := src[start:i]
		out = append(out, 0xFF, 0x00, byte(len(lit)>>8), byte(len(lit)))
		out = append(out, lit...)
	}
	return out, nil
}

func (r RLECodec) Decode(src []byte, rawLen int) ([]byte, error) {
	out := make([]byte, 0, rawLen)
	for i := 0; i < len(src); {
		if i+2 > len(src) {
			return nil, fmt.Errorf("compresslab: truncated rle pair")
		}
		count, val := src[i], src[i+1]
		i += 2
		if count == 0xFF && val == 0x00 {
			if i+2 > len(src) {
				return nil, fmt.Errorf("compresslab: truncated rle literal header")
			}
			n := int(src[i])<<8 | int(src[i+1])
			i += 2
			if i+n > len(src) {
				return nil, fmt.Errorf("compresslab: truncated rle literal")
			}
			out = append(out, src[i:i+n]...)
			i += n
			continue
		}
		for k := 0; k < int(count); k++ {
			out = append(out, val)
		}
	}
	if len(out) != rawLen {
		return nil, fmt.Errorf("compresslab: rle block decoded to %d bytes, want %d", len(out), rawLen)
	}
	return out, nil
}

func init() {
	Register(FlateCodec{})
	Register(RLECodec{})
}
