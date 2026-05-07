package store

import (
	"context"
	"os"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/storx/badgerx"
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
	if err := s.clearLogIndexes(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.segmentsDir); err != nil {
		return wrapExternal(err, "clear segment files")
	}
	return wrapExternal(os.MkdirAll(s.segmentsDir, 0o750), "recreate segment directory")
}

func (s *StorxLogStore) clearLogIndexes() error {
	for _, namespace := range []interface{ Clear(context.Context) error }{
		badgerNamespaceClearer[string, TopicConfig]{s.topics},
		badgerNamespaceClearer[string, segmentRecordPointer]{s.records},
		badgerNamespaceClearer[string, uint64]{s.nextOffsets},
	} {
		if err := namespace.Clear(context.Background()); err != nil {
			return wrapExternal(err, "clear log index")
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
	frame, err := encodeSegmentFrame(record)
	if err != nil {
		return err
	}
	pointer, err := s.appendFrame(topic, tp, record.Offset, frame, record)
	if err != nil {
		return err
	}
	return wrapExternal(s.records.Set(context.Background(), recordKey(tp, record.Offset), pointer), "restore log record index")
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
		if err := s.nextOffsets.Set(context.Background(), nextOffsetKey(tp), next); err != nil {
			resultErr = wrapExternal(err, "restore next log offset")
			return false
		}
		return true
	})
	return resultErr
}

type badgerNamespaceClearer[K any, V any] struct {
	namespace *badgerx.Namespace[K, V]
}

func (c badgerNamespaceClearer[K, V]) Clear(ctx context.Context) error {
	keys, err := scanBadgerNamespaceKeys(ctx, c.namespace)
	if err != nil {
		return wrapExternal(err, "list badger namespace keys")
	}
	for _, key := range keys {
		if err := c.namespace.Delete(ctx, key); err != nil {
			return wrapExternal(err, "delete badger namespace key")
		}
	}
	return nil
}
