package store

import (
	"errors"
	"os"
)

type segmentReader struct {
	file   *os.File
	mapped *mappedSegment
	mode   SegmentReadMode
}

func (s *StorxLogStore) readSegmentRecords(relativePath string, pointers []segmentRecordPointer) ([]Record, error) {
	reader, err := s.segmentReader(relativePath, s.segmentReadModeForPointers(pointers))
	if err != nil {
		return nil, err
	}
	return reader.readRecords(pointers)
}

func (s *StorxLogStore) segmentReader(relativePath string, mode SegmentReadMode) (*segmentReader, error) {
	s.readersMu.Lock()
	defer s.readersMu.Unlock()
	key := segmentReaderKey(relativePath, mode)
	if reader, ok := s.readers.Get(key); ok {
		return reader, nil
	}
	reader, err := openSegmentReader(s.segmentsDir, relativePath, mode)
	if err != nil {
		return nil, err
	}
	s.readers.Set(key, reader)
	return reader, nil
}

func (s *StorxLogStore) segmentReadModeForPointers(pointers []segmentRecordPointer) SegmentReadMode {
	if s.readMode != SegmentReadModeMmap || len(pointers) == 0 {
		return SegmentReadModePread
	}
	pointer := pointers[0]
	last, ok, err := s.lastRecordPointer(NewTopicPartition(pointer.Topic, pointer.Partition))
	if err != nil || !ok || last.SegmentID == pointer.SegmentID {
		return SegmentReadModePread
	}
	return SegmentReadModeMmap
}

func segmentReaderKey(relativePath string, mode SegmentReadMode) string {
	return string(mode) + "\x00" + relativePath
}

func openSegmentReader(rootDir, relativePath string, mode SegmentReadMode) (*segmentReader, error) {
	if mode == SegmentReadModeMmap {
		return openMappedSegmentReader(rootDir, relativePath)
	}
	return openPreadSegmentReader(rootDir, relativePath)
}

func openPreadSegmentReader(rootDir, relativePath string) (*segmentReader, error) {
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
	return &segmentReader{file: file, mode: SegmentReadModePread}, nil
}

func openMappedSegmentReader(rootDir, relativePath string) (*segmentReader, error) {
	mapped, err := openMappedSegment(rootDir, relativePath)
	if err != nil {
		if errors.Is(err, errSegmentMmapUnsupported) {
			return openPreadSegmentReader(rootDir, relativePath)
		}
		return nil, err
	}
	return &segmentReader{mapped: mapped, mode: SegmentReadModeMmap}, nil
}

func (r *segmentReader) readRecords(pointers []segmentRecordPointer) ([]Record, error) {
	if r == nil {
		return nil, E(CodeUnavailable, "segment reader is nil")
	}
	if r.mapped != nil {
		return readMappedSegmentRecords(r.mapped.data, pointers)
	}
	if r.file == nil {
		return nil, E(CodeUnavailable, "segment reader has no file")
	}
	return readSegmentRecords(r.file, pointers)
}

func (r *segmentReader) close() error {
	if r == nil {
		return nil
	}
	return errors.Join(
		closeMappedSegment(r.mapped),
		closeSegmentReaderFile(r.file),
	)
}

func closeSegmentReaderFile(file *os.File) error {
	if file == nil {
		return nil
	}
	return wrapExternal(file.Close(), "close segment file")
}

func (s *StorxLogStore) closeSegmentReaders() error {
	if s == nil || s.readers == nil {
		return nil
	}
	s.readersMu.Lock()
	defer s.readersMu.Unlock()
	var result error
	s.readers.Range(func(_ string, reader *segmentReader) bool {
		if reader != nil {
			result = errors.Join(result, reader.close())
		}
		return true
	})
	s.readers.Clear()
	return result
}
