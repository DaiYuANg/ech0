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
	return s.restoreLogRecords(snapshot.Records)
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

func (s *StorxLogStore) restoreLogRecords(records collectionmapping.Map[string, []Record]) error {
	nextOffsets := collectionmapping.NewMap[TopicPartition, uint64]()
	if err := s.restoreLogRecordValues(records, nextOffsets); err != nil {
		return err
	}
	return s.restoreLogNextOffsets(nextOffsets)
}

func (s *StorxLogStore) restoreLogRecordValues(records collectionmapping.Map[string, []Record], nextOffsets *collectionmapping.Map[TopicPartition, uint64]) error {
	var resultErr error
	records.Range(func(key string, topicRecords []Record) bool {
		tp, err := parsePartitionKey(key)
		if err != nil {
			resultErr = err
			return false
		}
		for _, record := range topicRecords {
			if err := s.records.Put(context.Background(), recordKey(tp, record.Offset), cloneRecord(record)); err != nil {
				resultErr = wrapExternal(err, "restore log record")
				return false
			}
			if record.Offset+1 > nextOffsets.GetOrDefault(tp, 0) {
				nextOffsets.Set(tp, record.Offset+1)
			}
		}
		return true
	})
	return resultErr
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
