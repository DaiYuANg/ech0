package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (s *StorxLogStore) EnforceRetention(ctx context.Context, nowMS uint64) (RetentionCleanupResult, error) {
	if err := ctx.Err(); err != nil {
		return RetentionCleanupResult{}, wrapExternal(err, "enforce segment retention")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := RetentionCleanupResult{}
	topics := s.listLogTopics()
	for i := range topics {
		topic := topics[i]
		for partition := range topic.Partitions {
			removed, removeErr := s.enforcePartitionRetention(ctx, topic, NewTopicPartition(topic.Name, partition), nowMS)
			if removeErr != nil {
				return result, removeErr
			}
			result.RemovedRecords += removed
		}
	}
	return result, nil
}

func (s *StorxLogStore) Compact(ctx context.Context, nowMS uint64, sealedSegmentBatch int) (CompactionCleanupResult, error) {
	_ = sealedSegmentBatch
	if err := ctx.Err(); err != nil {
		return CompactionCleanupResult{}, wrapExternal(err, "compact segment records")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	topics := s.listLogTopics()
	result, err := s.compactTopics(ctx, topics, nowMS)
	if err != nil {
		return CompactionCleanupResult{}, err
	}
	return result, nil
}

func (s *StorxLogStore) compactTopics(ctx context.Context, topics []TopicConfig, nowMS uint64) (CompactionCleanupResult, error) {
	result := CompactionCleanupResult{}
	for i := range topics {
		topic := topics[i]
		for partition := range topic.Partitions {
			removed, compactErr := s.compactPartition(ctx, topic, NewTopicPartition(topic.Name, partition), nowMS)
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

func (s *StorxLogStore) enforcePartitionRetention(ctx context.Context, topic TopicConfig, tp TopicPartition, nowMS uint64) (int, error) {
	records, err := s.partitionRecords(ctx, tp)
	if err != nil {
		return 0, err
	}
	remove := retentionRemovableOffsets(topic, records, nowMS)
	if remove.IsEmpty() {
		return 0, nil
	}
	return s.deleteRecordPointers(tp, remove)
}

func (s *StorxLogStore) compactPartition(ctx context.Context, topic TopicConfig, tp TopicPartition, nowMS uint64) (int, error) {
	records, err := s.partitionRecords(ctx, tp)
	if err != nil {
		return 0, err
	}
	remove := compactionRemovableOffsets(topic, records, nowMS)
	if remove.IsEmpty() {
		return 0, nil
	}
	return s.deleteRecordPointers(tp, remove)
}

func (s *StorxLogStore) listLogTopics() []TopicConfig {
	s.indexMu.RLock()
	topics := collectionlist.NewListWithCapacity[TopicConfig](s.topics.Len())
	s.topics.Range(func(_ string, topic TopicConfig) bool {
		topics.Add(cloneTopic(topic))
		return true
	})
	s.indexMu.RUnlock()
	return topics.Values()
}

func (s *StorxLogStore) partitionRecords(ctx context.Context, tp TopicPartition) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, wrapExternal(err, "list partition log records")
	}
	pointers := s.recordPointersFrom(tp, 0, maxSegmentInt())
	records, err := s.readPointers(pointers)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (s *StorxLogStore) deleteRecordPointers(tp TopicPartition, remove interface{ Contains(uint64) bool }) (int, error) {
	removed := s.removeRecordPointers(tp, remove)
	if len(removed) == 0 {
		return 0, nil
	}
	if err := s.appendSegmentIndexDeletes(removed); err != nil {
		return 0, wrapExternal(err, "delete compacted log indexes")
	}
	return len(removed), nil
}
