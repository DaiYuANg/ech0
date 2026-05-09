package store

import (
	"os"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *StorxLogStore) Restore(snapshot Snapshot) error {
	if err := s.clearLogStorage(); err != nil {
		return err
	}
	if err := s.restoreLogTopics(snapshot.Topics); err != nil {
		return err
	}
	return s.restoreLogRecords(snapshot)
}

func (s *StorxLogStore) clearLogStorage() error {
	if err := s.closeSegmentWriters(); err != nil {
		return err
	}
	if err := s.closeSegmentReaders(); err != nil {
		return err
	}
	if err := s.clearLogIndexes(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.segmentsDir); err != nil {
		return wrapExternal(err, "clear segment files")
	}
	return wrapExternal(os.MkdirAll(s.segmentsDir, 0o750), "recreate segment directory")
}

func (s *StorxLogStore) clearLogIndexes() error {
	s.indexMu.Lock()
	s.topics = collectionmapping.NewMap[string, TopicConfig]()
	s.records = collectionmapping.NewMap[TopicPartition, []segmentRecordPointer]()
	s.nextOffsets = collectionmapping.NewMap[TopicPartition, uint64]()
	s.indexMu.Unlock()
	if err := os.Remove(s.segmentManifestPath()); err != nil && !os.IsNotExist(err) {
		return wrapExternal(err, "clear log manifest")
	}
	return nil
}

func (s *StorxLogStore) restoreLogTopics(topics collectionlist.List[TopicConfig]) error {
	var resultErr error
	topics.Range(func(_ int, topic TopicConfig) bool {
		if err := s.CreateTopic(topic); err != nil {
			resultErr = err
			return false
		}
		return true
	})
	return resultErr
}

func (s *StorxLogStore) restoreLogRecords(snapshot Snapshot) error {
	nextOffsets := restoreLogNextOffsetMap(snapshot.LogOffsets)
	shouldComputeOffsets := nextOffsets.IsEmpty()
	if err := s.restoreLogRecordValues(snapshot.Records, nextOffsets, shouldComputeOffsets); err != nil {
		return err
	}
	return s.restoreLogNextOffsets(nextOffsets)
}

func (s *StorxLogStore) restoreLogRecordValues(records collectionmapping.Map[string, []Record], nextOffsets *collectionmapping.Map[TopicPartition, uint64], computeOffsets bool) error {
	var resultErr error
	records.Range(func(key string, topicRecords []Record) bool {
		if err := s.restoreLogPartitionRecords(key, topicRecords, nextOffsets, computeOffsets); err != nil {
			resultErr = err
			return false
		}
		return true
	})
	return resultErr
}

func (s *StorxLogStore) restoreLogPartitionRecords(
	key string,
	records []Record,
	nextOffsets *collectionmapping.Map[TopicPartition, uint64],
	computeOffsets bool,
) error {
	tp, err := parsePartitionKey(key)
	if err != nil {
		return err
	}
	for _, record := range records {
		if err := s.appendRestoredRecord(tp, cloneRecord(record)); err != nil {
			return err
		}
		advanceRestoredNextOffset(tp, record, nextOffsets, computeOffsets)
	}
	return nil
}

func (s *StorxLogStore) appendRestoredRecord(tp TopicPartition, record Record) error {
	topic, err := s.loadTopicForPartition(tp)
	if err != nil {
		return err
	}
	frame, err := encodeSegmentFrameWithCompression(record, s.compression)
	if err != nil {
		return err
	}
	pointer, err := s.appendFrame(topic, tp, record.Offset, frame, record)
	if err != nil {
		return err
	}
	if err := s.appendSegmentIndexPointers(s.segmentRelativePath(tp, pointer.SegmentID), []segmentRecordPointer{pointer}); err != nil {
		return wrapExternal(err, "restore log record index")
	}
	s.recordAppendedPointer(tp, pointer)
	return nil
}

func advanceRestoredNextOffset(
	tp TopicPartition,
	record Record,
	nextOffsets *collectionmapping.Map[TopicPartition, uint64],
	computeOffsets bool,
) {
	if computeOffsets && record.Offset >= nextOffsets.GetOrDefault(tp, 0) {
		nextOffsets.Set(tp, record.Offset+1)
	}
}

func restoreLogNextOffsetMap(snapshotOffsets collectionmapping.Map[string, uint64]) *collectionmapping.Map[TopicPartition, uint64] {
	nextOffsets := collectionmapping.NewMap[TopicPartition, uint64]()
	snapshotOffsets.Range(func(key string, value uint64) bool {
		tp, err := parsePartitionKey(key)
		if err == nil {
			nextOffsets.Set(tp, value)
		}
		return true
	})
	return nextOffsets
}

func (s *StorxLogStore) restoreLogNextOffsets(nextOffsets *collectionmapping.Map[TopicPartition, uint64]) error {
	s.indexMu.Lock()
	nextOffsets.Range(func(tp TopicPartition, next uint64) bool {
		s.nextOffsets.Set(tp, next)
		return true
	})
	s.indexMu.Unlock()
	return nil
}
