package datacompression

import (
	"context"
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
