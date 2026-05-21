package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type PartitionSnapshot struct {
	Topic          TopicConfig
	TopicPartition TopicPartition
	Records        []Record
	NextOffset     uint64
}

func (s *StorxLogStore) SnapshotPartition(tp TopicPartition) (PartitionSnapshot, error) {
	topic, err := s.loadTopicForPartition(tp)
	if err != nil {
		return PartitionSnapshot{}, err
	}
	lock := s.partitionLock(tp)
	lock.Lock()
	defer lock.Unlock()
	s.indexMu.RLock()
	pointers := cloneSegmentPointers(s.records.GetOrDefault(tp, nil))
	next := s.nextOffsets.GetOrDefault(tp, nextOffsetFromPointers(pointers))
	s.indexMu.RUnlock()
	records, err := s.readPointers(pointers)
	if err != nil {
		return PartitionSnapshot{}, err
	}
	return PartitionSnapshot{
		Topic:          cloneTopic(topic),
		TopicPartition: tp,
		Records:        cloneRecords(records),
		NextOffset:     next,
	}, nil
}

func (s *StorxLogStore) ReplacePartition(snapshot PartitionSnapshot) error {
	tp := snapshot.TopicPartition
	if err := s.ensurePartitionSnapshotTopic(snapshot); err != nil {
		return err
	}
	lock := s.partitionLock(tp)
	lock.Lock()
	defer lock.Unlock()
	if err := s.clearPartitionLocked(tp); err != nil {
		return err
	}
	for _, record := range sortedRecords(snapshot.Records) {
		if err := s.appendRestoredRecord(tp, cloneRecord(record)); err != nil {
			return err
		}
	}
	next := max(snapshot.NextOffset, nextOffsetFromRecords(snapshot.Records))
	s.indexMu.Lock()
	s.nextOffsets.Set(tp, next)
	s.indexMu.Unlock()
	return nil
}

func (s *StorxLogStore) ClearPartition(tp TopicPartition) error {
	if _, err := s.loadTopicForPartition(tp); err != nil {
		return err
	}
	lock := s.partitionLock(tp)
	lock.Lock()
	defer lock.Unlock()
	return s.clearPartitionLocked(tp)
}

func (s *StorxLogStore) ensurePartitionSnapshotTopic(snapshot PartitionSnapshot) error {
	tp := snapshot.TopicPartition
	if snapshot.Topic.Name != "" && snapshot.Topic.Name != tp.Topic {
		return E(CodeInvalidArgument, "partition snapshot topic %s does not match %s", snapshot.Topic.Name, tp.Topic)
	}
	if _, err := s.loadTopicForPartition(tp); err == nil {
		return nil
	} else if ErrorCode(err) != CodeTopicNotFound {
		return err
	}
	if snapshot.Topic.Name == "" {
		return E(CodeTopicNotFound, "topic %s not found", tp.Topic)
	}
	if err := s.CreateTopic(snapshot.Topic); err != nil && ErrorCode(err) != CodeTopicExists {
		return err
	}
	_, err := s.loadTopicForPartition(tp)
	return err
}

func (s *StorxLogStore) clearPartitionLocked(tp TopicPartition) error {
	if err := s.closePartitionSegments(tp); err != nil {
		return err
	}
	s.indexMu.Lock()
	s.records.Set(tp, nil)
	s.timestampRecords.Set(tp, nil)
	s.keyRecords.Delete(tp)
	s.keyIndexReady.Remove(tp)
	s.nextOffsets.Set(tp, 0)
	s.indexMu.Unlock()
	if err := os.RemoveAll(s.partitionDir(tp)); err != nil {
		return wrapExternal(err, "clear partition segment files")
	}
	return wrapExternal(os.MkdirAll(s.partitionDir(tp), 0o750), "recreate partition segment directory")
}

func (s *StorxLogStore) closePartitionSegments(tp TopicPartition) error {
	return errors.Join(
		s.closePartitionSegmentWriters(tp),
		s.closePartitionSegmentIndexWriters(tp),
		s.closePartitionSegmentReaders(tp),
	)
}

func (s *StorxLogStore) closePartitionSegmentWriters(tp TopicPartition) error {
	if s == nil || s.writers == nil {
		return nil
	}
	prefix := partitionRelativeDir(tp)
	s.writersMu.Lock()
	defer s.writersMu.Unlock()
	var result error
	remove := make([]string, 0)
	s.writers.Range(func(relativePath string, writer *segmentWriter) bool {
		if !partitionRelativePathMatches(relativePath, prefix) {
			return true
		}
		result = errors.Join(result, closePartitionSegmentWriter(writer))
		remove = append(remove, relativePath)
		return true
	})
	for _, relativePath := range remove {
		s.writers.Delete(relativePath)
	}
	return result
}

func (s *StorxLogStore) closePartitionSegmentIndexWriters(tp TopicPartition) error {
	if s == nil || s.indexWriters == nil {
		return nil
	}
	prefix := partitionRelativeDir(tp)
	s.indexWritersMu.Lock()
	defer s.indexWritersMu.Unlock()
	var result error
	remove := make([]string, 0)
	s.indexWriters.Range(func(relativePath string, writer *segmentIndexWriter) bool {
		if !partitionRelativePathMatches(relativePath, prefix) {
			return true
		}
		result = errors.Join(result, closePartitionSegmentIndexWriter(writer))
		remove = append(remove, relativePath)
		return true
	})
	for _, relativePath := range remove {
		s.indexWriters.Delete(relativePath)
	}
	return result
}

func (s *StorxLogStore) closePartitionSegmentReaders(tp TopicPartition) error {
	if s == nil || s.readers == nil {
		return nil
	}
	prefix := partitionRelativeDir(tp)
	s.readersMu.Lock()
	defer s.readersMu.Unlock()
	var result error
	remove := make([]string, 0)
	s.readers.Range(func(key string, reader *segmentReader) bool {
		if !partitionReaderKeyMatches(key, prefix) {
			return true
		}
		result = errors.Join(result, reader.close())
		remove = append(remove, key)
		return true
	})
	for _, key := range remove {
		s.readers.Delete(key)
	}
	return result
}

func closePartitionSegmentWriter(writer *segmentWriter) error {
	if writer == nil || writer.file == nil {
		return nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	err := errors.Join(
		wrapExternal(writer.file.Sync(), "sync segment file"),
		wrapExternal(writer.file.Close(), "close segment file"),
	)
	writer.file = nil
	return err
}

func closePartitionSegmentIndexWriter(writer *segmentIndexWriter) error {
	if writer == nil || writer.file == nil {
		return nil
	}
	writer.mu.Lock()
	defer writer.mu.Unlock()
	err := errors.Join(
		wrapExternal(writer.file.Sync(), "sync segment index"),
		wrapExternal(writer.file.Close(), "close segment index"),
	)
	writer.file = nil
	return err
}

func partitionReaderKeyMatches(key, partitionRelativeDir string) bool {
	_, relativePath, ok := strings.Cut(key, "\x00")
	return ok && partitionRelativePathMatches(relativePath, partitionRelativeDir)
}

func partitionRelativePathMatches(relativePath, partitionRelativeDir string) bool {
	return relativePath == partitionRelativeDir || strings.HasPrefix(relativePath, partitionRelativeDir+string(filepath.Separator))
}
