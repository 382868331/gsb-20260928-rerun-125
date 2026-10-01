package datacompression

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

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

func partialPath(path string) string    { return path + ".partial" }
func checkpointPath(path string) string { return path + ".checkpoint" }

// CompressFileResumable compresses into path while keeping resumable
// progress on disk. Frames are written to path+".partial" and a checkpoint
// is saved to path+".checkpoint" after every confirmed block. On success the
// partial output is renamed to path and the checkpoint removed. On
// cancellation or failure both the partial output and the checkpoint are
// kept as interruption evidence so ResumeFile can continue later.
func CompressFileResumable(ctx context.Context, path string, r io.Reader, opts Options) error {
	if path == "" {
		return ErrInvalidArgument
	}
	partial := partialPath(path)
	file, err := os.Create(partial)
	if err != nil {
		return err
	}
	if err := runPipeline(ctx, file, r, opts, nil, checkpointSaver(checkpointPath(path))); err != nil {
		_ = file.Close()
		return err
	}
	return finalizePartial(file, partial, path)
}

// ResumeFile continues an interrupted CompressFileResumable run. The
// checkpoint and the input are validated before anything is written; when
// they do not match the current opts or input, the partial output and the
// checkpoint are left untouched and ErrIncompatibleState is returned. Any
// unconfirmed tail beyond the checkpoint is discarded, never treated as
// complete.
func ResumeFile(ctx context.Context, path string, r io.Reader, opts Options) error {
	if path == "" {
		return ErrInvalidArgument
	}
	cp, err := loadCheckpointForResume(checkpointPath(path))
	if err != nil {
		return err
	}
	partial := partialPath(path)
	file, err := os.OpenFile(partial, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return err
	}
	if info.Size() < cp.OutputOffset {
		_ = file.Close()
		return fmt.Errorf("%w: partial output shorter than checkpoint", ErrIncompatibleState)
	}
	if err := file.Truncate(cp.OutputOffset); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Seek(cp.OutputOffset, io.SeekStart); err != nil {
		_ = file.Close()
		return err
	}
	if err := runPipeline(ctx, file, r, opts, &cp, checkpointSaver(checkpointPath(path))); err != nil {
		_ = file.Close()
		return err
	}
	return finalizePartial(file, partial, path)
}

func finalizePartial(file *os.File, partial, path string) (err error) {
	defer func() {
		if err != nil {
			_ = file.Close()
		}
	}()
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(partial, path); err != nil {
		return err
	}
	if err = os.Remove(checkpointPath(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if dirFile, dirErr := os.Open(filepath.Dir(path)); dirErr == nil {
		defer dirFile.Close()
		_ = dirFile.Sync()
	}
	return nil
}
