package store

import (
	"context"

	"github.com/arcgolabs/storx/badgerx"
)

func (s *StorxLogStore) EnforceRetention(nowMS uint64) (RetentionCleanupResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := RetentionCleanupResult{}
	topics, err := s.listLogTopics()
	if err != nil {
		return result, err
	}
	for i := range topics {
		topic := topics[i]
		for partition := range topic.Partitions {
			removed, removeErr := s.enforcePartitionRetention(topic, NewTopicPartition(topic.Name, partition), nowMS)
			if removeErr != nil {
				return result, removeErr
			}
			result.RemovedRecords += removed
		}
	}
	return result, nil
}

func (s *StorxLogStore) Compact(nowMS uint64, sealedSegmentBatch int) (CompactionCleanupResult, error) {
	_ = sealedSegmentBatch
	s.mu.Lock()
	defer s.mu.Unlock()
	result := CompactionCleanupResult{}
	topics, err := s.listLogTopics()
	if err != nil {
		return result, err
	}
	for i := range topics {
		topic := topics[i]
		for partition := range topic.Partitions {
			removed, compactErr := s.compactPartition(topic, NewTopicPartition(topic.Name, partition), nowMS)
			if compactErr != nil {
				return result, compactErr
			}
			if removed > 0 {
				result.CompactedPartitions++
				result.RemovedRecords += removed
			}
		}
	}
	return result, nil
}

func (s *StorxLogStore) enforcePartitionRetention(topic TopicConfig, tp TopicPartition, nowMS uint64) (int, error) {
	entries, records, err := s.partitionRecordEntries(tp)
	if err != nil {
		return 0, err
	}
	remove := retentionRemovableOffsets(topic, records, nowMS)
	if remove.IsEmpty() {
		return 0, nil
	}
	return s.deleteRecordEntries(entries, remove)
}

func (s *StorxLogStore) compactPartition(topic TopicConfig, tp TopicPartition, nowMS uint64) (int, error) {
	entries, records, err := s.partitionRecordEntries(tp)
	if err != nil {
		return 0, err
	}
	remove := compactionRemovableOffsets(topic, records, nowMS)
	if remove.IsEmpty() {
		return 0, nil
	}
	return s.deleteRecordEntries(entries, remove)
}

func (s *StorxLogStore) listLogTopics() ([]TopicConfig, error) {
	entries, err := scanBadgerNamespace(context.Background(), s.topics)
	if err != nil {
		return nil, wrapExternal(err, "walk log topics")
	}
	topics := make([]TopicConfig, 0, len(entries))
	for _, entry := range entries {
		topics = append(topics, cloneTopic(entry.Value))
	}
	return topics, nil
}

func (s *StorxLogStore) partitionRecordEntries(tp TopicPartition) ([]badgerx.Entry[string, segmentRecordPointer], []Record, error) {
	entries, err := s.records.List(
		context.Background(),
		badgerx.WithPrefix[string]([]byte(recordPrefix(tp))),
	)
	if err != nil {
		return nil, nil, wrapExternal(err, "list partition log records")
	}
	records, err := s.recordsFromPointers(entries)
	if err != nil {
		return nil, nil, err
	}
	return entries, records, nil
}

func (s *StorxLogStore) deleteRecordEntries(entries []badgerx.Entry[string, segmentRecordPointer], remove interface{ Contains(uint64) bool }) (int, error) {
	deleted := 0
	for _, entry := range entries {
		if !remove.Contains(entry.Value.Offset) {
			continue
		}
		if err := s.records.Delete(context.Background(), entry.Key); err != nil {
			return deleted, wrapExternal(err, "delete compacted log index")
		}
		deleted++
	}
	return deleted, nil
}
