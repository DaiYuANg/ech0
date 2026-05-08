package store

import (
	"errors"
	"os"
)

type segmentReader struct {
	file *os.File
}

func (s *StorxLogStore) readSegmentRecords(relativePath string, pointers []segmentRecordPointer) ([]Record, error) {
	reader, err := s.segmentReader(relativePath)
	if err != nil {
		return nil, err
	}
	return readSegmentRecords(reader.file, pointers)
}

func (s *StorxLogStore) segmentReader(relativePath string) (*segmentReader, error) {
	s.readersMu.Lock()
	defer s.readersMu.Unlock()
	if reader, ok := s.readers.Get(relativePath); ok {
		return reader, nil
	}
	reader, err := openSegmentReader(s.segmentsDir, relativePath)
	if err != nil {
		return nil, err
	}
	s.readers.Set(relativePath, reader)
	return reader, nil
}

func openSegmentReader(rootDir, relativePath string) (*segmentReader, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment root")
	}
	file, err := root.Open(relativePath)
	if err != nil {
		return nil, errors.Join(wrapExternal(err, "open segment file"), wrapExternal(root.Close(), "close segment root"))
	}
	if err := root.Close(); err != nil {
		return nil, errors.Join(wrapExternal(err, "close segment root"), wrapExternal(file.Close(), "close segment file"))
	}
	return &segmentReader{file: file}, nil
}

func (s *StorxLogStore) closeSegmentReaders() error {
	if s == nil || s.readers == nil {
		return nil
	}
	s.readersMu.Lock()
	defer s.readersMu.Unlock()
	var result error
	s.readers.Range(func(_ string, reader *segmentReader) bool {
		if reader != nil && reader.file != nil {
			result = errors.Join(result, wrapExternal(reader.file.Close(), "close segment file"))
		}
		return true
	})
	s.readers.Clear()
	return result
}
