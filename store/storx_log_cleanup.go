package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/badgerx"
)

const segmentIndexValueLogGCDiscardRatio = 0.5

func (s *StorxLogStore) EnforceRetention(ctx context.Context, nowMS uint64) (RetentionCleanupResult, error) {
	if err := ctx.Err(); err != nil {
		return RetentionCleanupResult{}, wrapExternal(err, "enforce storx retention")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	result := RetentionCleanupResult{}
	topics, err := s.listLogTopics(ctx)
	if err != nil {
		return result, err
	}
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
	if result.RemovedRecords > 0 {
		return result, s.runSegmentIndexValueLogGC(ctx)
	}
	return result, nil
}

func (s *StorxLogStore) Compact(ctx context.Context, nowMS uint64, sealedSegmentBatch int) (CompactionCleanupResult, error) {
	_ = sealedSegmentBatch
	if err := ctx.Err(); err != nil {
		return CompactionCleanupResult{}, wrapExternal(err, "compact storx records")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	topics, err := s.listLogTopics(ctx)
	if err != nil {
		return CompactionCleanupResult{}, err
	}
	result, err := s.compactTopics(ctx, topics, nowMS)
	if err != nil {
		return CompactionCleanupResult{}, err
	}
	if result.RemovedRecords > 0 {
		return result, s.runSegmentIndexValueLogGC(ctx)
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

func (s *StorxLogStore) runSegmentIndexValueLogGC(ctx context.Context) error {
	if s == nil || s.index == nil {
		return nil
	}
	return wrapExternal(s.index.RunValueLogGC(ctx, segmentIndexValueLogGCDiscardRatio), "run segment index value log gc")
}

func (s *StorxLogStore) enforcePartitionRetention(ctx context.Context, topic TopicConfig, tp TopicPartition, nowMS uint64) (int, error) {
	entries, records, err := s.partitionRecordEntries(ctx, tp)
	if err != nil {
		return 0, err
	}
	remove := retentionRemovableOffsets(topic, records, nowMS)
	if remove.IsEmpty() {
		return 0, nil
	}
	return s.deleteRecordEntries(ctx, entries, remove)
}

func (s *StorxLogStore) compactPartition(ctx context.Context, topic TopicConfig, tp TopicPartition, nowMS uint64) (int, error) {
	entries, records, err := s.partitionRecordEntries(ctx, tp)
	if err != nil {
		return 0, err
	}
	remove := compactionRemovableOffsets(topic, records, nowMS)
	if remove.IsEmpty() {
		return 0, nil
	}
	return s.deleteRecordEntries(ctx, entries, remove)
}

func (s *StorxLogStore) listLogTopics(ctx context.Context) ([]TopicConfig, error) {
	entries, err := scanBadgerNamespace(ctx, s.topics)
	if err != nil {
		return nil, wrapExternal(err, "walk log topics")
	}
	topics := collectionlist.NewListWithCapacity[TopicConfig](len(entries))
	for _, entry := range entries {
		topics.Add(cloneTopic(entry.Value))
	}
	return topics.Values(), nil
}

func (s *StorxLogStore) partitionRecordEntries(ctx context.Context, tp TopicPartition) ([]badgerx.Entry[recordIndexKey, segmentRecordPointer], []Record, error) {
	prefix, err := recordIndexPrefix(tp)
	if err != nil {
		return nil, nil, err
	}
	entries, err := s.records.List(
		ctx,
		badgerx.WithPrefix[recordIndexKey](prefix),
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

func (s *StorxLogStore) deleteRecordEntries(ctx context.Context, entries []badgerx.Entry[recordIndexKey, segmentRecordPointer], remove interface{ Contains(uint64) bool }) (int, error) {
	keys := collectionlist.NewList[recordIndexKey]()
	for _, entry := range entries {
		if !remove.Contains(entry.Value.Offset) {
			continue
		}
		keys.Add(entry.Key)
	}
	if err := s.records.DeleteMany(ctx, keys.Values()...); err != nil {
		return 0, wrapExternal(err, "delete compacted log indexes")
	}
	return keys.Len(), nil
}
