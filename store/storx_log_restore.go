package store

import (
	"context"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *StorxLogStore) Restore(snapshot Snapshot) error {
	if err := s.clearLogBuckets(); err != nil {
		return err
	}
	if err := s.restoreLogTopics(snapshot.Topics); err != nil {
		return err
	}
	return s.restoreLogRecords(snapshot)
}

func (s *StorxLogStore) clearLogBuckets() error {
	for _, bucket := range []interface{ Clear(context.Context) error }{
		bucketClearer[string, TopicConfig]{s.topics},
		bucketClearer[string, Record]{s.records},
		bucketClearer[string, uint64]{s.nextOffsets},
	} {
		if err := bucket.Clear(context.Background()); err != nil {
			return wrapExternal(err, "clear log bucket")
		}
	}
	return nil
}

func (s *StorxLogStore) restoreLogTopics(topics []TopicConfig) error {
	for i := range topics {
		if err := s.CreateTopic(topics[i]); err != nil {
			return err
		}
	}
	return nil
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
		if err := s.records.Put(context.Background(), recordKey(tp, record.Offset), cloneRecord(record)); err != nil {
			return wrapExternal(err, "restore log record")
		}
		advanceRestoredNextOffset(tp, record, nextOffsets, computeOffsets)
	}
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
	var resultErr error
	nextOffsets.Range(func(tp TopicPartition, next uint64) bool {
		if err := s.nextOffsets.Put(context.Background(), nextOffsetKey(tp), next); err != nil {
			resultErr = wrapExternal(err, "restore next log offset")
			return false
		}
		return true
	})
	return resultErr
}
