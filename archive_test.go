package datacompression

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCompressAndReadBlocksIndependently(t *testing.T) {
	parts := [][]byte{
		bytes.Repeat([]byte("abcdefgh"), 16),
		make([]byte, 128),
		bytes.Repeat([]byte("tail-con"), 16),
	}
	input := bytes.Join(parts, nil)

	var archive bytes.Buffer
	if err := Compress(context.Background(), &archive, bytes.NewReader(input), Options{
		BlockSize:   128,
		Concurrency: 3,
	}); err != nil {
		t.Fatalf("Compress() error = %v", err)
	}

	reader, err := NewReaderFromBytes(archive.Bytes(), nil)
	if err != nil {
		t.Fatalf("NewReaderFromBytes() error = %v", err)
	}
	if len(reader.Entries()) != 3 {
		t.Fatalf("entry count = %d, want 3", len(reader.Entries()))
	}
	for i, want := range parts {
		blockReader, err := NewReaderFromBytes(archive.Bytes(), nil)
		if err != nil {
			t.Fatalf("NewReaderFromBytes(%d) error = %v", i, err)
		}
		got, err := blockReader.ReadBlock(i)
		if err != nil {
			t.Fatalf("ReadBlock(%d) error = %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadBlock(%d) mismatch", i)
		}
	}
}

func TestSelectorPicksFlateAndGrowthFallsBackToStore(t *testing.T) {
	compressible := bytes.Repeat([]byte("platform"), 512)
	var compressed bytes.Buffer
	if err := Compress(context.Background(), &compressed, bytes.NewReader(compressible), Options{BlockSize: len(compressible)}); err != nil {
		t.Fatal(err)
	}
	compressedReader, err := NewReaderFromBytes(compressed.Bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := compressedReader.Entries()[0].Codec; got != CodecFlate {
		t.Fatalf("compressible codec = %d, want flate", got)
	}

	randomData := make([]byte, 4096)
	if _, err := rand.Read(randomData); err != nil {
		t.Fatal(err)
	}
	growing := growingCodec{}
	var stored bytes.Buffer
	if err := Compress(context.Background(), &stored, bytes.NewReader(randomData), Options{
		BlockSize:   len(randomData),
		Concurrency: 1,
		Codecs:      []Codec{storeCodec{}, growing},
		Selector:    func(Block, []Codec) Codec { return growing },
	}); err != nil {
		t.Fatal(err)
	}
	storedReader, err := NewReaderFromBytes(stored.Bytes(), []Codec{growing})
	if err != nil {
		t.Fatal(err)
	}
	entry := storedReader.Entries()[0]
	if entry.Codec != CodecStore {
		t.Fatalf("growing codec = %d, want store", entry.Codec)
	}
	if entry.CompressedLength != int64(frameHeaderSize+len(randomData)) {
		t.Fatalf("compressed length = %d, want original block size", entry.CompressedLength)
	}
}

func TestParallelResultsPublishInOriginalOrder(t *testing.T) {
	codec := orderedCodec{}
	input := bytes.Repeat([]byte{0, 1, 2, 3, 4, 5, 6, 7}, 1024)
	var archive bytes.Buffer
	err := Compress(context.Background(), &archive, bytes.NewReader(input), Options{
		BlockSize:   16,
		Concurrency: 4,
		Codecs:      []Codec{storeCodec{}, codec},
		Selector: func(block Block, _ []Codec) Codec {
			if block.Seq == 0 {
				return slowFirstCodec{orderedCodec{}}
			}
			return codec
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderFromBytes(archive.Bytes(), []Codec{codec})
	if err != nil {
		t.Fatal(err)
	}
	for i, entry := range reader.Entries() {
		if entry.Seq != i {
			t.Fatalf("entry order = %d at %d", entry.Seq, i)
		}
	}
}

func TestCancellationLeavesNoCompleteArtifact(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	codec := blockingCodec{release: release}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	var archive bytes.Buffer
	go func() {
		done <- Compress(ctx, &archive, bytes.NewReader(bytes.Repeat([]byte("x"), 128)), Options{
			BlockSize:   16,
			Concurrency: 2,
			Codecs:      []Codec{storeCodec{}, codec},
			Selector:    func(Block, []Codec) Codec { return codec },
		})
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Compress did not return after cancellation")
	}
	if _, err := NewReaderFromBytes(archive.Bytes(), nil); !errors.Is(err, ErrBadArchive) {
		t.Fatalf("partial archive error = %v, want ErrBadArchive", err)
	}

	target := filepath.Join(t.TempDir(), "archive.gcbk")
	fileCtx, fileCancel := context.WithCancel(context.Background())
	fileDone := make(chan error, 1)
	go func() {
		fileDone <- CompressFile(fileCtx, target, bytes.NewReader(bytes.Repeat([]byte("y"), 128)), Options{
			BlockSize:   16,
			Concurrency: 2,
			Codecs:      []Codec{storeCodec{}, codec},
			Selector:    func(Block, []Codec) Codec { return codec },
		})
	}()
	time.Sleep(20 * time.Millisecond)
	fileCancel()
	if err := <-fileDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("CompressFile error = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("target file after cancellation err = %v, want ErrNotExist", err)
	}
}

func TestCorruptIndexAndTruncatedArchiveRejected(t *testing.T) {
	input := bytes.Repeat([]byte("independent-block"), 100)
	var archive bytes.Buffer
	if err := Compress(context.Background(), &archive, bytes.NewReader(input), Options{BlockSize: 64, Concurrency: 2}); err != nil {
		t.Fatal(err)
	}
	valid := append([]byte(nil), archive.Bytes()...)
	footer := valid[len(valid)-footerSize:]
	indexOffset := int64(uint64(footer[8]) | uint64(footer[9])<<8)

	corrupt := append([]byte(nil), valid...)
	corrupt[indexOffset+indexHeaderSize] ^= 0xff
	if _, err := NewReaderFromBytes(corrupt, nil); !errors.Is(err, ErrBadArchive) {
		t.Fatalf("corrupt index error = %v, want ErrBadArchive", err)
	}

	for _, cut := range []int{len(valid) - 1, len(valid) - footerSize, int(indexOffset)} {
		if _, err := NewReaderFromBytes(valid[:cut], nil); err == nil {
			t.Fatalf("truncated archive at %d was accepted", cut)
		}
	}
}

func TestNewCodecDoesNotBreakOldData(t *testing.T) {
	input := bytes.Repeat([]byte("legacy"), 20)
	var archive bytes.Buffer
	if err := Compress(context.Background(), &archive, bytes.NewReader(input), Options{BlockSize: 120}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderFromBytes(archive.Bytes(), []Codec{futureCodec{}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := reader.ReadBlock(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != strings.Repeat("legacy", 20) {
		t.Fatal("old block could not be decoded after registering future codec")
	}
}

func TestEmptyArchiveAndUnknownCodec(t *testing.T) {
	var empty bytes.Buffer
	if err := Compress(context.Background(), &empty, bytes.NewReader(nil), Options{}); err != nil {
		t.Fatal(err)
	}
	emptyReader, err := NewReaderFromBytes(empty.Bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(emptyReader.Entries()) != 0 {
		t.Fatal("empty archive should contain no entries")
	}

	codec := futureCodec{}
	var archive bytes.Buffer
	if err := Compress(context.Background(), &archive, bytes.NewReader([]byte("abcdefghijklmnop")), Options{
		BlockSize: 16,
		Codecs:    []Codec{storeCodec{}, codec},
		Selector:  func(Block, []Codec) Codec { return codec },
	}); err != nil {
		t.Fatal(err)
	}
	reader, err := NewReaderFromBytes(archive.Bytes(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.ReadBlock(0); !errors.Is(err, ErrUnknownCodec) {
		t.Fatalf("unknown codec error = %v, want ErrUnknownCodec", err)
	}
}

type growingCodec struct{}

func (growingCodec) ID() uint16 {
	return 200
}
func (growingCodec) Name() string { return "growing" }
func (growingCodec) Encode(src []byte) ([]byte, error) {
	return bytes.Repeat([]byte{0xff}, len(src)+16), nil
}
func (growingCodec) Decode(src []byte) ([]byte, error) { return src, nil }

type orderedCodec struct{}

func (orderedCodec) ID() uint16 {
	return 201
}
func (orderedCodec) Name() string { return "ordered" }
func (orderedCodec) Encode(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	for i := range dst {
		dst[i]++
	}
	return dst, nil
}
func (orderedCodec) Decode(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	for i := range dst {
		dst[i]--
	}
	return dst, nil
}

type slowFirstCodec struct{ orderedCodec }

func (s slowFirstCodec) Encode(src []byte) ([]byte, error) {
	time.Sleep(30 * time.Millisecond)
	return s.orderedCodec.Encode(src)
}

type blockingCodec struct {
	release <-chan struct{}
}

func (blockingCodec) ID() uint16   { return 202 }
func (blockingCodec) Name() string { return "blocking" }
func (b blockingCodec) Encode(src []byte) ([]byte, error) {
	<-b.release
	return src, nil
}
func (blockingCodec) Decode(src []byte) ([]byte, error) { return src, nil }

type futureCodec struct{}

func (futureCodec) ID() uint16   { return 203 }
func (futureCodec) Name() string { return "future" }
func (futureCodec) Encode(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	for i := range dst {
		dst[i]++
	}
	return dst, nil
}
func (futureCodec) Decode(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	for i := range dst {
		dst[i]--
	}
	return dst, nil
}

func TestInFlightBoundedWhenHeadBlockSlow(t *testing.T) {
	const concurrency = 4
	const blockCount = 24
	blockSize := 200
	blocks := make([][]byte, blockCount)
	for i := range blocks {
		blocks[i] = make([]byte, blockSize)
		for j := range blocks[i] {
			blocks[i][j] = []byte{byte(i + 1), byte(i*3 + 2), byte(i*7 + 3)}[j%3]
		}
	}
	blocks[blockCount-1] = blocks[blockCount-1][:37]

	gate := make(chan struct{})
	input := &chunkReader{blocks: blocks}
	var archive frameCountingBuffer
	codec := &trackedOrderedCodec{id: 210}
	done := make(chan error, 1)
	go func() {
		done <- Compress(context.Background(), &archive, input, Options{
			BlockSize:   blockSize,
			Concurrency: concurrency,
			Codecs:      []Codec{storeCodec{}, codec, gatedCodec{gate: gate, codec: codec, id: 211}},
			Selector: func(block Block, _ []Codec) Codec {
				if block.Seq == 0 {
					return gatedCodec{gate: gate, codec: codec, id: 211}
				}
				return codec
			},
		})
	}()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if input.readCount.Load() == concurrency {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	if got := input.readCount.Load(); got != concurrency {
		t.Fatalf("read blocks with head stalled = %d, want %d", got, concurrency)
	}
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("Compress() error = %v", err)
	}
	if peak := codec.peakActive.Load(); peak > concurrency {
		t.Fatalf("peak concurrent encodes = %d, want <= %d", peak, concurrency)
	}
	verifyIndependentDecoding(t, archive.Bytes(), blocks, codec, gatedCodec{gate: gate, codec: codec, id: 211})
}

func TestInFlightBoundedAcrossSlowAndUnevenBlocks(t *testing.T) {
	const concurrency = 3
	const blockCount = 18
	blockSize := 160
	cases := []struct {
		name string
		slow int
	}{
		{"last block slow", blockCount - 1},
		{"middle block slow", blockCount / 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blocks := make([][]byte, blockCount)
			for i := range blocks {
				size := blockSize
				if i == blockCount-1 {
					size = 13
				}
				blocks[i] = make([]byte, size)
				if i%3 == 0 {
					for j := range blocks[i] {
						blocks[i][j] = byte(i + 1)
					}
				} else {
					for j := range blocks[i] {
						blocks[i][j] = byte(i*31 + j*7)
					}
				}
			}

			gate := make(chan struct{})
			input := &chunkReader{blocks: blocks}
			var archive frameCountingBuffer
			codec := &trackedOrderedCodec{id: 220}
			samplerDone := make(chan struct{})
			samplerStop := make(chan struct{})
			var peakInFlight atomic.Int64
			go func() {
				defer close(samplerDone)
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-samplerStop:
						return
					case <-ticker.C:
						inFlight := input.readCount.Load() - archive.frames.Load()
						for {
							old := peakInFlight.Load()
							if inFlight <= old || peakInFlight.CompareAndSwap(old, inFlight) {
								break
							}
						}
					}
				}
			}()

			go func() {
				time.Sleep(60 * time.Millisecond)
				close(gate)
			}()
			err := Compress(context.Background(), &archive, input, Options{
				BlockSize:   blockSize,
				Concurrency: concurrency,
				Codecs:      []Codec{storeCodec{}, codec, gatedCodec{gate: gate, codec: codec, id: 221}},
				Selector: func(block Block, _ []Codec) Codec {
					if block.Seq == tc.slow {
						return gatedCodec{gate: gate, codec: codec, id: 221}
					}
					return codec
				},
			})
			close(samplerStop)
			<-samplerDone
			if err != nil {
				t.Fatalf("Compress() error = %v", err)
			}
			if peak := peakInFlight.Load(); peak > concurrency {
				t.Fatalf("peak in-flight blocks = %d, want <= %d", peak, concurrency)
			}
			if peak := codec.peakActive.Load(); peak > concurrency {
				t.Fatalf("peak concurrent encodes = %d, want <= %d", peak, concurrency)
			}
			verifyIndependentDecoding(t, archive.Bytes(), blocks, codec, gatedCodec{gate: gate, codec: codec, id: 221})
		})
	}
}

func TestEncodeFailureStopsPipelineAndCleansArtifact(t *testing.T) {
	const concurrency = 2
	errEncode := errors.New("encode exploded")
	input := &chunkReader{blocks: makeTestBlocks(40, 128)}
	var archive bytes.Buffer
	failing := failingCodec{err: errEncode}
	err := Compress(context.Background(), &archive, input, Options{
		BlockSize:   128,
		Concurrency: concurrency,
		Codecs:      []Codec{storeCodec{}, failing},
		Selector:    func(Block, []Codec) Codec { return failing },
	})
	if !errors.Is(err, errEncode) {
		t.Fatalf("Compress() error = %v, want %v", err, errEncode)
	}
	time.Sleep(50 * time.Millisecond)
	stalled := input.readCount.Load()
	time.Sleep(50 * time.Millisecond)
	if got := input.readCount.Load(); got != stalled {
		t.Fatalf("reader kept advancing after failure: %d then %d", stalled, got)
	}
	if _, openErr := NewReaderFromBytes(archive.Bytes(), nil); !errors.Is(openErr, ErrBadArchive) {
		t.Fatalf("partial archive error = %v, want ErrBadArchive", openErr)
	}

	target := filepath.Join(t.TempDir(), "archive.gcbk")
	fileInput := &chunkReader{blocks: makeTestBlocks(40, 128)}
	err = CompressFile(context.Background(), target, fileInput, Options{
		BlockSize:   128,
		Concurrency: concurrency,
		Codecs:      []Codec{storeCodec{}, failing},
		Selector:    func(Block, []Codec) Codec { return failing },
	})
	if !errors.Is(err, errEncode) {
		t.Fatalf("CompressFile() error = %v, want %v", err, errEncode)
	}
	if _, statErr := os.Stat(target); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("target file after failure err = %v, want ErrNotExist", statErr)
	}
}

func makeTestBlocks(count, size int) [][]byte {
	blocks := make([][]byte, count)
	for i := range blocks {
		blocks[i] = bytes.Repeat([]byte{byte(i + 1), byte(i*7 + 2)}, (size+1)/2)[:size]
	}
	return blocks
}

func verifyIndependentDecoding(t *testing.T, data []byte, blocks [][]byte, extras ...Codec) {
	t.Helper()
	for i, want := range blocks {
		reader, err := NewReaderFromBytes(data, extras)
		if err != nil {
			t.Fatalf("NewReaderFromBytes(%d) error = %v", i, err)
		}
		got, err := reader.ReadBlock(i)
		if err != nil {
			t.Fatalf("ReadBlock(%d) error = %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("ReadBlock(%d) data mismatch", i)
		}
	}
}

type chunkReader struct {
	blocks    [][]byte
	next      int
	readCount atomic.Int64
}

func (c *chunkReader) Read(p []byte) (int, error) {
	if c.next >= len(c.blocks) {
		return 0, io.EOF
	}
	if cap(p) < len(c.blocks[c.next]) {
		return 0, io.ErrShortBuffer
	}
	block := c.blocks[c.next]
	c.next++
	n := copy(p, block)
	c.readCount.Add(1)
	return n, nil
}

type frameCountingBuffer struct {
	bytes.Buffer
	frames atomic.Int64
}

func (b *frameCountingBuffer) Write(p []byte) (int, error) {
	if len(p) >= 4 && binary.LittleEndian.Uint32(p[:4]) == frameMagic {
		b.frames.Add(1)
	}
	return b.Buffer.Write(p)
}

type trackedOrderedCodec struct {
	id         uint16
	active     atomic.Int64
	peakActive atomic.Int64
}

func (c *trackedOrderedCodec) ID() uint16   { return c.id }
func (c *trackedOrderedCodec) Name() string { return "tracked-ordered" }
func (c *trackedOrderedCodec) Encode(src []byte) ([]byte, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		peak := c.peakActive.Load()
		if active <= peak || c.peakActive.CompareAndSwap(peak, active) {
			break
		}
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	for i := range dst {
		dst[i]++
	}
	return dst, nil
}
func (c *trackedOrderedCodec) Decode(src []byte) ([]byte, error) {
	dst := make([]byte, len(src))
	copy(dst, src)
	for i := range dst {
		dst[i]--
	}
	return dst, nil
}

type gatedCodec struct {
	gate  <-chan struct{}
	codec *trackedOrderedCodec
	id    uint16
}

func (g gatedCodec) ID() uint16   { return g.id }
func (g gatedCodec) Name() string { return "gated-ordered" }
func (g gatedCodec) Encode(src []byte) ([]byte, error) {
	<-g.gate
	return g.codec.Encode(src)
}
func (g gatedCodec) Decode(src []byte) ([]byte, error) { return g.codec.Decode(src) }

type failingCodec struct {
	err error
}

func (failingCodec) ID() uint16   { return 212 }
func (failingCodec) Name() string { return "failing" }
func (c failingCodec) Encode([]byte) ([]byte, error) {
	return nil, c.err
}
func (c failingCodec) Decode(src []byte) ([]byte, error) { return src, nil }
