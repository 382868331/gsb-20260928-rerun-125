package datacompression

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	partialSuffix    = ".partial"
	checkpointSuffix = ".checkpoint"
)

// PartialPath returns the path where interrupted partial output is kept.
func PartialPath(path string) string { return path + partialSuffix }

// CheckpointPath returns the path where the resume checkpoint is kept.
func CheckpointPath(path string) string { return path + checkpointSuffix }

func CompressFile(ctx context.Context, path string, r io.Reader, opts Options) (err error) {
	if path == "" {
		return ErrInvalidArgument
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".compress-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() {
		if err != nil {
			os.Remove(tmpPath)
		}
	}()

	if err = Compress(ctx, tmp, r, opts); err != nil {
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
	if err = os.Rename(tmpPath, path); err != nil {
		return err
	}
	if dirFile, dirErr := os.Open(dir); dirErr == nil {
		defer dirFile.Close()
		_ = dirFile.Sync()
	}
	return nil
}

// CompressFileResumable behaves like CompressFile, but on cancellation or
// failure it preserves the confirmed prefix of the output at
// PartialPath(path) and records a Checkpoint at CheckpointPath(path) so the run
// can be continued later with ResumeCompressFile. The target path is only
// ever replaced by a complete archive.
func CompressFileResumable(ctx context.Context, path string, r io.Reader, opts Options) error {
	if path == "" {
		return ErrInvalidArgument
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".compress-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	cw := &countingWriter{w: tmp}
	checkpoint, err := runCompression(ctx, cw, r, opts, nil)
	if err != nil {
		_ = tmp.Close()
		// Preserve the partial output and the interruption evidence instead
		// of deleting them, so the run can be resumed or inspected.
		_ = os.Remove(PartialPath(path))
		if renameErr := os.Rename(tmpPath, PartialPath(path)); renameErr != nil {
			os.Remove(tmpPath)
			return errors.Join(err, renameErr)
		}
		if checkpoint != nil {
			if saveErr := SaveCheckpoint(CheckpointPath(path), checkpoint); saveErr != nil {
				return errors.Join(err, saveErr)
			}
		}
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		os.Remove(tmpPath)
		return err
	}
	if err = tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err = os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	_ = os.Remove(PartialPath(path))
	_ = os.Remove(CheckpointPath(path))
	if dirFile, dirErr := os.Open(dir); dirErr == nil {
		defer dirFile.Close()
		_ = dirFile.Sync()
	}
	return nil
}

// ResumeCompressFile continues an interrupted CompressFileResumable run. It
// verifies that the new input and options are compatible with the saved
// checkpoint, discards any unconfirmed tail beyond the confirmed prefix, and
// appends only the remaining blocks. On success the completed archive
// replaces path and the checkpoint is removed; on failure the partial output
// and an updated checkpoint are kept for another attempt.
func ResumeCompressFile(ctx context.Context, path string, r io.Reader, opts Options) (err error) {
	if path == "" {
		return ErrInvalidArgument
	}
	checkpoint, err := LoadCheckpoint(CheckpointPath(path))
	if err != nil {
		return err
	}
	if err := checkpoint.CompatibleWith(opts); err != nil {
		return err
	}

	partial, err := os.OpenFile(PartialPath(path), os.O_RDWR, 0)
	if err != nil {
		return err
	}
	info, err := partial.Stat()
	if err != nil {
		_ = partial.Close()
		return err
	}
	if info.Size() < checkpoint.DataEnd || checkpoint.DataEnd < headerSize {
		_ = partial.Close()
		return fmt.Errorf("%w: partial output shorter than confirmed prefix", ErrBadArchive)
	}
	header := make([]byte, headerSize)
	if _, err := partial.ReadAt(header, 0); err != nil {
		_ = partial.Close()
		return err
	}
	if string(header[:4]) != archiveMagic {
		_ = partial.Close()
		return fmt.Errorf("%w: partial output header", ErrBadArchive)
	}
	// Writing resumes at DataEnd; the unconfirmed tail beyond it is
	// overwritten by new frames and finally truncated on success. If the
	// input or settings turn out to be incompatible, nothing is written
	// and the partial output is left untouched as evidence.
	if _, err := partial.Seek(checkpoint.DataEnd, io.SeekStart); err != nil {
		_ = partial.Close()
		return err
	}

	cw := &countingWriter{w: partial, n: checkpoint.DataEnd}
	updated, err := runCompression(ctx, cw, r, opts, checkpoint)
	if err != nil {
		_ = partial.Close()
		if updated != nil {
			if saveErr := SaveCheckpoint(CheckpointPath(path), updated); saveErr != nil {
				return errors.Join(err, saveErr)
			}
		}
		return err
	}
	if err = partial.Truncate(cw.n); err != nil {
		_ = partial.Close()
		return err
	}
	if err = partial.Sync(); err != nil {
		_ = partial.Close()
		return err
	}
	if err = partial.Close(); err != nil {
		return err
	}
	if err = os.Rename(PartialPath(path), path); err != nil {
		return err
	}
	_ = os.Remove(CheckpointPath(path))
	if dirFile, dirErr := os.Open(filepath.Dir(path)); dirErr == nil {
		defer dirFile.Close()
		_ = dirFile.Sync()
	}
	return nil
}

func OpenReader(path string, codecs []Codec) (*Reader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	reader, err := NewReader(file, info.Size(), codecs)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	reader.closer = file
	return reader, nil
}
