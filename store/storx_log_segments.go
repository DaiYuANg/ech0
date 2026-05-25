package store

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/lyonbrown4d/ech0/internal/bufferpool"
)

func (s *StorxLogStore) nextOffset(topicPartition TopicPartition) uint64 {
	s.indexMu.RLock()
	offset, ok := s.nextOffsets.Get(topicPartition)
	s.indexMu.RUnlock()
	if !ok {
		return 0
	}
	return offset
}

func (s *StorxLogStore) appendFrame(
	topic TopicConfig,
	topicPartition TopicPartition,
	offset uint64,
	frame []byte,
	record Record,
) (segmentRecordPointer, error) {
	segmentID, err := s.segmentIDForAppend(topic, topicPartition, offset, len(frame))
	if err != nil {
		return segmentRecordPointer{}, err
	}
	result, err := s.appendFramesToSegment(topicPartition, segmentID, [][]byte{frame})
	if err != nil {
		return segmentRecordPointer{}, err
	}
	return segmentRecordPointer{
		Topic:       topicPartition.Topic,
		Partition:   topicPartition.Partition,
		Offset:      record.Offset,
		SegmentID:   segmentID,
		Position:    result.positions[0],
		Length:      len(frame),
		TimestampMS: record.TimestampMS,
		Attributes:  record.Attributes,
	}, nil
}

type segmentAppendResult struct {
	relativePath string
	positions    []int64
}

func (s *StorxLogStore) appendFramesToSegment(topicPartition TopicPartition, segmentID uint64, frames [][]byte) (segmentAppendResult, error) {
	relativePath := s.segmentRelativePath(topicPartition, segmentID)
	if mkdirErr := os.MkdirAll(filepath.Join(s.segmentsDir, filepath.Dir(relativePath)), 0o750); mkdirErr != nil {
		return segmentAppendResult{}, wrapExternal(mkdirErr, "create segment file directory")
	}
	writer, err := s.segmentWriter(relativePath)
	if err != nil {
		return segmentAppendResult{}, err
	}
	positions, err := writer.appendFrames(frames)
	if err != nil {
		return segmentAppendResult{}, err
	}
	return segmentAppendResult{relativePath: relativePath, positions: positions}, nil
}

type segmentWriter struct {
	mu       sync.Mutex
	file     *os.File
	position int64
}

func (s *StorxLogStore) segmentWriter(relativePath string) (*segmentWriter, error) {
	s.writersMu.Lock()
	defer s.writersMu.Unlock()
	if writer, ok := s.writers.Get(relativePath); ok {
		return writer, nil
	}
	writer, err := openSegmentWriter(s.segmentsDir, relativePath)
	if err != nil {
		return nil, err
	}
	s.writers.Set(relativePath, writer)
	return writer, nil
}

func openSegmentWriter(rootDir, relativePath string) (*segmentWriter, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment root")
	}
	file, err := root.OpenFile(relativePath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.Join(wrapExternal(err, "open segment file"), wrapExternal(root.Close(), "close segment root"))
	}
	closeRootErr := root.Close()
	position, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, errors.Join(wrapExternal(err, "seek segment file"), wrapExternal(file.Close(), "close segment file"), wrapExternal(closeRootErr, "close segment root"))
	}
	if closeRootErr != nil {
		return nil, errors.Join(wrapExternal(closeRootErr, "close segment root"), wrapExternal(file.Close(), "close segment file"))
	}
	return &segmentWriter{file: file, position: position}, nil
}

func (w *segmentWriter) appendFrames(frames [][]byte) ([]int64, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	positions, payload, release := w.coalesceFrames(frames)
	defer release()
	if err := w.writeFrames(payload); err != nil {
		return nil, err
	}
	w.position += int64(len(payload))
	return positions, nil
}

func (w *segmentWriter) coalesceFrames(frames [][]byte) ([]int64, []byte, func()) {
	positions := make([]int64, 0, len(frames))
	total := 0
	nextPosition := w.position
	for _, frame := range frames {
		positions = append(positions, nextPosition)
		nextPosition += int64(len(frame))
		total += len(frame)
	}
	if len(frames) == 1 {
		return positions, frames[0], func() {}
	}
	buffer := bufferpool.Get()
	if cap(buffer.B) < total {
		buffer.B = make([]byte, 0, total)
	}
	for _, frame := range frames {
		buffer.B = append(buffer.B, frame...)
	}
	return positions, buffer.B, func() { bufferpool.Put(buffer) }
}

func (w *segmentWriter) writeFrames(payload []byte) error {
	written, err := w.file.Write(payload)
	if err != nil {
		return wrapExternal(err, "write segment record")
	}
	if written != len(payload) {
		return wrapExternal(io.ErrShortWrite, "write segment record")
	}
	return nil
}

func (w *segmentWriter) sync() error {
	if w == nil || w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return wrapExternal(w.file.Sync(), "sync segment file")
}

func (s *StorxLogStore) closeSegmentWriters() error {
	if s == nil || s.writers == nil {
		return nil
	}
	s.writersMu.Lock()
	defer s.writersMu.Unlock()
	var result error
	s.writers.Range(func(_ string, writer *segmentWriter) bool {
		if writer != nil && writer.file != nil {
			result = errors.Join(result, writer.sync(), wrapExternal(writer.file.Close(), "close segment file"))
		}
		return true
	})
	s.writers.Clear()
	return result
}

func statSegmentFile(rootDir, relativePath string) (os.FileInfo, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment root")
	}
	info, err := root.Stat(relativePath)
	if err != nil {
		return nil, errors.Join(err, wrapExternal(root.Close(), "close segment root"))
	}
	if err := root.Close(); err != nil {
		return nil, wrapExternal(err, "close segment root")
	}
	return info, nil
}

func segmentHasCapacity(currentSize int64, frameSize int, maxBytes uint64) (bool, error) {
	used, err := nonNegativeInt64ToUint64(currentSize, "segment file size")
	if err != nil {
		return false, err
	}
	next, err := nonNegativeIntToUint64(frameSize, "segment frame size")
	if err != nil {
		return false, err
	}
	return used <= maxBytes && next <= maxBytes-used, nil
}

func nonNegativeInt64ToUint64(value int64, name string) (uint64, error) {
	if value < 0 {
		return 0, E(CodeCodec, "%s is negative: %d", name, value)
	}
	return uint64(value), nil
}

func nonNegativeIntToUint64(value int, name string) (uint64, error) {
	if value < 0 {
		return 0, E(CodeCodec, "%s is negative: %d", name, value)
	}
	return uint64(value), nil
}

func (s *StorxLogStore) segmentIDForAppend(
	topic TopicConfig,
	topicPartition TopicPartition,
	offset uint64,
	frameSize int,
) (uint64, error) {
	last, ok, err := s.lastRecordPointer(topicPartition)
	if err != nil {
		return 0, err
	}
	if !ok {
		return offset, nil
	}
	maxBytes := topic.SegmentMaxBytes
	if maxBytes == 0 {
		return last.SegmentID, nil
	}
	info, err := statSegmentFile(s.segmentsDir, s.segmentRelativePath(topicPartition, last.SegmentID))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return offset, nil
		}
		return 0, wrapExternal(err, "stat segment file")
	}
	hasCapacity, err := segmentHasCapacity(info.Size(), frameSize, maxBytes)
	if err != nil {
		return 0, err
	}
	if hasCapacity {
		return last.SegmentID, nil
	}
	return offset, nil
}

func (s *StorxLogStore) lastRecordPointer(topicPartition TopicPartition) (segmentRecordPointer, bool, error) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	pointers := s.records.GetOrDefault(topicPartition, nil)
	if len(pointers) == 0 {
		return segmentRecordPointer{}, false, nil
	}
	return pointers[len(pointers)-1], true, nil
}
