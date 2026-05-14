//go:build !windows

package store

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func openMappedSegment(rootDir, relativePath string) (*mappedSegment, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open mmap segment root")
	}
	file, err := root.Open(relativePath)
	if err != nil {
		return nil, errors.Join(wrapExternal(err, "open mmap segment file"), wrapExternal(root.Close(), "close mmap segment root"))
	}
	if closeErr := root.Close(); closeErr != nil {
		return nil, closeMappedSegmentOpen(nil, file, wrapExternal(closeErr, "close mmap segment root"))
	}
	info, err := file.Stat()
	if err != nil {
		return nil, closeMappedSegmentOpen(nil, file, wrapExternal(err, "stat mmap segment file"))
	}
	size := info.Size()
	if size == 0 {
		return &mappedSegment{close: func() error { return wrapExternal(file.Close(), "close mmap segment file") }}, nil
	}
	if size < 0 || size != int64(int(size)) {
		return nil, closeMappedSegmentOpen(nil, file, E(CodeInvalidArgument, "segment file size %d cannot be memory-mapped", size))
	}
	fd := file.Fd()
	if fd > uintptr(^uint(0)>>1) {
		return nil, closeMappedSegmentOpen(nil, file, E(CodeInvalidArgument, "segment file descriptor %d cannot be memory-mapped", fd))
	}
	data, err := unix.Mmap(int(fd), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, closeMappedSegmentOpen(nil, file, wrapExternal(err, "memory-map segment file"))
	}
	return &mappedSegment{
		data: data,
		close: func() error {
			return closeMappedSegmentOpen(data, file, nil)
		},
	}, nil
}

func closeMappedSegmentOpen(data []byte, file *os.File, err error) error {
	if len(data) > 0 {
		err = errors.Join(err, wrapExternal(unix.Munmap(data), "unmap segment file"))
	}
	if file != nil {
		err = errors.Join(err, wrapExternal(file.Close(), "close mmap segment file"))
	}
	return err
}
