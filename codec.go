package datacompression

import (
	"bytes"
	"compress/flate"
	"io"
	"math"
)

const (
	CodecStore uint16 = 0
	CodecFlate uint16 = 1
)

type Codec interface {
	ID() uint16
	Name() string
	Encode(src []byte) ([]byte, error)
	Decode(src []byte) ([]byte, error)
}

type storeCodec struct{}

func (storeCodec) ID() uint16   { return CodecStore }
func (storeCodec) Name() string { return "store" }

func (storeCodec) Encode(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst, nil
}

func (storeCodec) Decode(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst, nil
}

type flateCodec struct {
	level int
}

func (flateCodec) ID() uint16   { return CodecFlate }
func (flateCodec) Name() string { return "flate" }

func (c flateCodec) Encode(src []byte) ([]byte, error) {
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, c.level)
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

func (flateCodec) Decode(src []byte) ([]byte, error) {
	r := flate.NewReader(bytes.NewReader(src))
	defer r.Close()
	return io.ReadAll(r)
}

func DefaultCodecs() []Codec {
	return []Codec{storeCodec{}, flateCodec{level: flate.DefaultCompression}}
}

type Selector func(block Block, candidates []Codec) Codec

func DefaultSelector(block Block, candidates []Codec) Codec {
	store := codecByID(candidates, CodecStore)
	flate := codecByID(candidates, CodecFlate)
	if store == nil {
		store = storeCodec{}
	}
	if flate == nil || len(block.Data) < 64 {
		return store
	}

	counts := [256]int{}
	sampleLimit := 64 * 1024
	if len(block.Data) < sampleLimit {
		sampleLimit = len(block.Data)
	}
	for _, value := range block.Data[:sampleLimit] {
		counts[value]++
	}

	zeroRatio := float64(counts[0]) / float64(sampleLimit)
	entropy := 0.0
	for _, count := range counts {
		if count == 0 {
			continue
		}
		probability := float64(count) / float64(sampleLimit)
		entropy -= probability * math.Log2(probability)
	}

	if zeroRatio >= 0.18 || entropy <= 6.5 {
		return flate
	}
	return store
}

func codecByID(codecs []Codec, id uint16) Codec {
	for _, codec := range codecs {
		if codec != nil && codec.ID() == id {
			return codec
		}
	}
	return nil
}
