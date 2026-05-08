package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/badgerx"
)

func (s *StorxLogStore) AppendRecordsBatch(topicPartition TopicPartition, records []RecordAppend) ([]Record, error) {
	out := collectionlist.NewListWithCapacity[Record](len(records))
	for _, record := range records {
		appended, err := s.AppendRecord(topicPartition, record)
		if err != nil {
			return nil, err
		}
		out.Add(appended)
	}
	return out.Values(), nil
}

func (s *StorxLogStore) ReadFrom(topicPartition TopicPartition, offset uint64, maxRecords int) ([]Record, error) {
	if maxRecords <= 0 {
		return nil, nil
	}
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return nil, err
	}
	prefix, err := recordIndexPrefix(topicPartition)
	if err != nil {
		return nil, err
	}
	entries, err := s.records.List(
		context.Background(),
		badgerx.WithPrefix[recordIndexKey](prefix),
		badgerx.WithStart(recordKey(topicPartition, offset)),
		badgerx.WithLimit[recordIndexKey](maxRecords),
	)
	if err != nil {
		return nil, wrapExternal(err, "list segment record indexes")
	}
	return s.recordsFromPointers(entries)
}

func (s *StorxLogStore) ReadPage(topicPartition TopicPartition, cursor string, maxRecords int) (RecordPage, error) {
	if maxRecords <= 0 {
		return RecordPage{}, nil
	}
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return RecordPage{}, err
	}
	prefix, err := recordIndexPrefix(topicPartition)
	if err != nil {
		return RecordPage{}, err
	}
	page, err := s.records.Page(
		context.Background(),
		cursor,
		badgerx.WithPrefix[recordIndexKey](prefix),
		badgerx.WithLimit[recordIndexKey](maxRecords),
	)
	if err != nil {
		return RecordPage{}, wrapExternal(err, "page segment record index")
	}
	records, err := s.recordsFromPointers(page.Entries)
	if err != nil {
		return RecordPage{}, err
	}
	return RecordPage{
		Records:    records,
		NextCursor: page.NextCursor,
		HasMore:    page.HasMore,
	}, nil
}

func (s *StorxLogStore) recordsFromPointers(entries []badgerx.Entry[recordIndexKey, segmentRecordPointer]) ([]Record, error) {
	out := collectionlist.NewListWithCapacity[Record](len(entries))
	for _, entry := range entries {
		record, err := s.readPointer(entry.Value)
		if err != nil {
			return nil, err
		}
		out.Add(cloneRecord(record))
	}
	return out.Values(), nil
}

func (s *StorxLogStore) readPointer(pointer segmentRecordPointer) (Record, error) {
	tp := NewTopicPartition(pointer.Topic, pointer.Partition)
	record, err := readSegmentRecord(s.segmentsDir, s.segmentRelativePath(tp, pointer.SegmentID), pointer.Position, pointer.Length)
	if err != nil {
		return Record{}, err
	}
	if record.Offset != pointer.Offset {
		return Record{}, E(CodeCodec, "segment record offset mismatch: index=%d record=%d", pointer.Offset, record.Offset)
	}
	return record, nil
}

func (s *StorxLogStore) LastOffset(topicPartition TopicPartition) (*uint64, error) {
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return nil, err
	}
	next, ok, err := s.nextOffsets.Get(context.Background(), nextOffsetKey(topicPartition))
	if err != nil {
		return nil, wrapExternal(err, "load next log offset")
	}
	if !ok || next == 0 {
		var absent *uint64
		return absent, nil
	}
	last := next - 1
	return &last, nil
}

func (s *StorxLogStore) loadTopicForPartition(topicPartition TopicPartition) (TopicConfig, error) {
	topic, ok, err := s.topics.Get(context.Background(), topicPartition.Topic)
	if err != nil {
		return TopicConfig{}, wrapExternal(err, "load log topic")
	}
	if !ok {
		return TopicConfig{}, E(CodeTopicNotFound, "topic %s not found", topicPartition.Topic)
	}
	if topicPartition.Partition >= topic.Partitions {
		return TopicConfig{}, E(CodePartitionNotFound, "partition %s/%d not found", topicPartition.Topic, topicPartition.Partition)
	}
	return cloneTopic(topic), nil
}
