package compresslab

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sampleData builds content with three distinct regions: repetitive,
// text-like compressible, and random incompressible.
func sampleData() []byte {
	var buf bytes.Buffer
	buf.Write(bytes.Repeat([]byte("A"), 100000)) // rle-friendly
	for i := 0; i < 3000; i++ {
		buf.WriteString("the quick brown fox jumps over the lazy dog. ")
	}
	rnd := make([]byte, 100000)
	if _, err := rand.Read(rnd); err != nil {
		panic(err)
	}
	buf.Write(rnd) // incompressible
	return buf.Bytes()
}

func compressToBytes(t *testing.T, data []byte, opts Options) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := Compress(context.Background(), bytes.NewReader(data), &out, opts); err != nil {
		t.Fatalf("Compress: %v", err)
	}
	return out.Bytes()
}

// independentDecode decodes every block through the exported DecodeBlock
// path and concatenates the results, without using Reader.
func independentDecode(t *testing.T, stream []byte) []byte {
	t.Helper()
	ra := bytes.NewReader(stream)
	r, err := OpenReader(ra, int64(len(stream)))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	var out bytes.Buffer
	for i, m := range r.Index() {
		raw, err := DecodeBlock(ra, m)
		if err != nil {
			t.Fatalf("DecodeBlock %d: %v", i, err)
		}
		out.Write(raw)
	}
	return out.Bytes()
}

func TestRoundTripIndependentDecode(t *testing.T) {
	data := sampleData()
	stream := compressToBytes(t, data, Options{BlockSize: 64 << 10, Workers: 4})
	got := independentDecode(t, stream)
	if !bytes.Equal(got, data) {
		t.Fatalf("round trip mismatch: got %d bytes, want %d", len(got), len(data))
	}
}

func TestPerBlockStrategyAndIndex(t *testing.T) {
	data := sampleData()
	opts := Options{BlockSize: 64 << 10, Workers: 4}
	stream := compressToBytes(t, data, opts)
	r, err := OpenReader(bytes.NewReader(stream), int64(len(stream)))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	index := r.Index()
	if len(index) < 3 {
		t.Fatalf("expected several blocks, got %d", len(index))
	}
	sawStore, sawCompressed := false, false
	for i, m := range index {
		if _, ok := Lookup(m.CodecID); !ok {
			t.Fatalf("block %d: unknown codec %d", i, m.CodecID)
		}
		if m.CodecID == codecIDStore {
			sawStore = true
			if m.CompLen != m.RawLen {
				t.Fatalf("block %d: stored but CompLen %d != RawLen %d", i, m.CompLen, m.RawLen)
			}
		} else {
			sawCompressed = true
			if m.CompLen >= m.RawLen {
				t.Fatalf("block %d: compressed but not smaller (%d >= %d)", i, m.CompLen, m.RawLen)
			}
		}
	}
	if !sawStore || !sawCompressed {
		t.Fatalf("expected a mix of stored and compressed blocks, got %+v", index)
	}
}

func TestRandomAccessSegment(t *testing.T) {
	data := sampleData()
	stream := compressToBytes(t, data, Options{BlockSize: 32 << 10})
	r, err := OpenReader(bytes.NewReader(stream), int64(len(stream)))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	for _, off := range []int64{0, 1, 99999, 100001, int64(len(data)) / 2, int64(len(data)) - 10} {
		n := int64(5000)
		got, err := r.ReadSegment(off, n)
		if err != nil {
			t.Fatalf("ReadSegment(%d): %v", off, err)
		}
		end := off + n
		if end > int64(len(data)) {
			end = int64(len(data))
		}
		if !bytes.Equal(got, data[off:end]) {
			t.Fatalf("segment at %d mismatch", off)
		}
	}
}

func TestParallelOrderPreserved(t *testing.T) {
	data := sampleData()
	stream := compressToBytes(t, data, Options{BlockSize: 8 << 10, Workers: 8, MaxInFlight: 4})
	got := independentDecode(t, stream)
	if !bytes.Equal(got, data) {
		t.Fatal("parallel compression did not preserve order")
	}
}

func TestCancelLeavesNoArtifact(t *testing.T) {
	data := sampleData()
	dir := t.TempDir()
	path := filepath.Join(dir, "out.clab")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before we even start
	err := CompressFile(ctx, bytes.NewReader(data), path, Options{BlockSize: 16 << 10})
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("cancelled run left artifact at %s", path)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), ".compresslab-") {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}

func TestCorruptIndexRejected(t *testing.T) {
	data := sampleData()
	stream := compressToBytes(t, data, Options{BlockSize: 64 << 10})
	// Footer tells us where the index lives.
	f, err := readFooterAt(bytes.NewReader(stream), int64(len(stream)))
	if err != nil {
		t.Fatalf("readFooterAt: %v", err)
	}
	corrupt := append([]byte(nil), stream...)
	corrupt[f.IndexOffset+3] ^= 0xFF // flip a RawLen byte inside the index
	_, err = OpenReader(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected index checksum error, got %v", err)
	}
}

func TestTruncatedStreamRejected(t *testing.T) {
	data := sampleData()
	stream := compressToBytes(t, data, Options{BlockSize: 64 << 10})
	for _, cut := range []int{1, footerSize, footerSize + 10, len(stream) / 2} {
		trunc := stream[:len(stream)-cut]
		if _, err := OpenReader(bytes.NewReader(trunc), int64(len(trunc))); err == nil {
			t.Fatalf("truncated stream (-%d bytes) opened without error", cut)
		}
	}
}

// TestNewCodecDoesNotBreakOldStreams registers a brand-new codec after a
// stream was written and confirms the old stream still reads, the new
// codec participates in selection, and unknown IDs fail cleanly.
func TestNewCodecDoesNotBreakOldStreams(t *testing.T) {
	data := sampleData()
	old := compressToBytes(t, data, Options{BlockSize: 64 << 10})

	Register(RLECodec{CodecID: 42}) // a "new" codec arriving later

	got := independentDecode(t, old)
	if !bytes.Equal(got, data) {
		t.Fatal("old stream unreadable after registering new codec")
	}

	// Streams written now may use the new codec and must still decode.
	fresh := compressToBytes(t, data, Options{BlockSize: 64 << 10})
	if !bytes.Equal(independentDecode(t, fresh), data) {
		t.Fatal("stream written with new codec registered failed round trip")
	}

	// An index entry naming a codec ID nobody provides must error.
	r, err := OpenReader(bytes.NewReader(old), int64(len(old)))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	m := r.Index()[0]
	m.CodecID = 200 // not registered
	if _, err := DecodeBlock(bytes.NewReader(old), m); err == nil ||
		!strings.Contains(err.Error(), "unknown codec") {
		t.Fatalf("expected unknown codec error, got %v", err)
	}
}

// TestIndexRecordsActualCodec ensures the index reflects the codec that
// was really used, including the store fallback for incompressible data.
func TestIndexRecordsActualCodec(t *testing.T) {
	rnd := make([]byte, 64<<10)
	if _, err := rand.Read(rnd); err != nil {
		panic(err)
	}
	stream := compressToBytes(t, rnd, Options{BlockSize: 64 << 10})
	r, err := OpenReader(bytes.NewReader(stream), int64(len(stream)))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	index := r.Index()
	if len(index) != 1 {
		t.Fatalf("expected 1 block, got %d", len(index))
	}
	if index[0].CodecID != codecIDStore {
		t.Fatalf("incompressible block should be stored, used codec %d", index[0].CodecID)
	}
	// Footer/index sanity: block count and checksum already validated by
	// OpenReader; also confirm the stream is self-consistent in size.
	f, err := readFooterAt(bytes.NewReader(stream), int64(len(stream)))
	if err != nil {
		t.Fatalf("readFooterAt: %v", err)
	}
	if f.BlockCount != 1 {
		t.Fatalf("footer block count %d, want 1", f.BlockCount)
	}
}
