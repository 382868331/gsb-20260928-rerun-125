package datacompression

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
