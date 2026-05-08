package store

import (
	"cmp"
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *StorxLogStore) Snapshot() (Snapshot, error) {
	topics, err := s.snapshotTopics()
	if err != nil {
		return Snapshot{}, err
	}
	nextOffsets, err := s.snapshotLogOffsets()
	if err != nil {
		return Snapshot{}, err
	}
	records, err := s.snapshotRecords()
	if err != nil {
		return Snapshot{}, err
	}
	return Snapshot{Topics: topics, Records: *records, LogOffsets: *nextOffsets}, nil
}

func (s *StorxLogStore) snapshotTopics() (collectionlist.List[TopicConfig], error) {
	entries, err := scanBadgerNamespace(context.Background(), s.topics)
	if err != nil {
		return collectionlist.List[TopicConfig]{}, wrapExternal(err, "walk log topics")
	}
	topics := collectionlist.NewListWithCapacity[TopicConfig](len(entries))
	for _, entry := range entries {
		topics.Add(cloneTopic(entry.Value))
	}
	return *topics.
		Sort(func(left, right TopicConfig) int {
			return cmp.Compare(left.Name, right.Name)
		}), nil
}

func (s *StorxLogStore) snapshotLogOffsets() (*collectionmapping.Map[string, uint64], error) {
	entries, err := scanBadgerNamespace(context.Background(), s.nextOffsets)
	if err != nil {
		return nil, wrapExternal(err, "walk log next offsets")
	}
	nextOffsets := collectionmapping.NewMap[string, uint64]()
	for _, entry := range entries {
		tp, err := entry.Key.topicPartition()
		if err != nil {
			return nil, err
		}
		nextOffsets.Set(partitionKey(tp), entry.Value)
	}
	return nextOffsets, nil
}

func (s *StorxLogStore) snapshotRecords() (*collectionmapping.Map[string, []Record], error) {
	entries, err := scanBadgerNamespace(context.Background(), s.records)
	if err != nil {
		return nil, wrapExternal(err, "walk segment record indexes")
	}
	records := collectionmapping.NewMap[string, []Record]()
	for _, entry := range entries {
		tp, err := entry.Key.topicPartition()
		if err != nil {
			return nil, err
		}
		record, err := s.readPointer(entry.Value)
		if err != nil {
			return nil, err
		}
		key := partitionKey(tp)
		topicRecords := records.GetOrDefault(key, nil)
		records.Set(key, append(topicRecords, cloneRecord(record)))
	}
	records.Range(func(key string, topicRecords []Record) bool {
		records.Set(key, sortedRecords(topicRecords))
		return true
	})
	return records, nil
}
