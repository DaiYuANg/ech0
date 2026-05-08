package store

import (
	"context"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/storx/badgerx"
)

func (s *StorxLogStore) ReadFrom(topicPartition TopicPartition, offset uint64, maxRecords int) (out []Record, err error) {
	const operation = "read_from"
	totalStart := time.Now()
	defer func() {
		s.recordReadStage(operation, "total", len(out), totalStart, err)
	}()
	if maxRecords <= 0 {
		return nil, nil
	}
	topicStart := time.Now()
	_, err = s.loadTopicForPartition(topicPartition)
	s.recordReadStage(operation, "load_topic", 0, topicStart, err)
	if err != nil {
		return nil, err
	}
	prefix, err := recordIndexPrefix(topicPartition)
	if err != nil {
		return nil, err
	}
	indexStart := time.Now()
	entries, err := s.records.List(
		context.Background(),
		badgerx.WithPrefix[recordIndexKey](prefix),
		badgerx.WithStart(recordKey(topicPartition, offset)),
		badgerx.WithLimit[recordIndexKey](maxRecords),
	)
	s.recordReadStage(operation, "index_list", len(entries), indexStart, err)
	if err != nil {
		return nil, wrapExternal(err, "list segment record indexes")
	}
	readStart := time.Now()
	out, err = s.recordsFromPointers(entries)
	s.recordReadStage(operation, "read_segments", len(out), readStart, err)
	return out, err
}

func (s *StorxLogStore) ReadPage(topicPartition TopicPartition, cursor string, maxRecords int) (out RecordPage, err error) {
	const operation = "read_page"
	totalStart := time.Now()
	defer func() {
		s.recordReadStage(operation, "total", len(out.Records), totalStart, err)
	}()
	if maxRecords <= 0 {
		return RecordPage{}, nil
	}
	topicStart := time.Now()
	_, err = s.loadTopicForPartition(topicPartition)
	s.recordReadStage(operation, "load_topic", 0, topicStart, err)
	if err != nil {
		return RecordPage{}, err
	}
	prefix, err := recordIndexPrefix(topicPartition)
	if err != nil {
		return RecordPage{}, err
	}
	pageStart := time.Now()
	page, err := s.records.Page(
		context.Background(),
		cursor,
		badgerx.WithPrefix[recordIndexKey](prefix),
		badgerx.WithLimit[recordIndexKey](maxRecords),
	)
	s.recordReadStage(operation, "index_page", len(page.Entries), pageStart, err)
	if err != nil {
		return RecordPage{}, wrapExternal(err, "page segment record index")
	}
	readStart := time.Now()
	records, err := s.recordsFromPointers(page.Entries)
	s.recordReadStage(operation, "read_segments", len(records), readStart, err)
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
	pointers := make([]segmentRecordPointer, len(entries))
	for index, entry := range entries {
		pointers[index] = entry.Value
	}
	return s.readPointers(pointers)
}

func (s *StorxLogStore) readPointers(pointers []segmentRecordPointer) ([]Record, error) {
	out := make([]Record, len(pointers))
	groups := collectionmapping.NewMap[string, []int]()
	for index, pointer := range pointers {
		tp := NewTopicPartition(pointer.Topic, pointer.Partition)
		relativePath := s.segmentRelativePath(tp, pointer.SegmentID)
		groups.Set(relativePath, append(groups.GetOrDefault(relativePath, nil), index))
	}
	var resultErr error
	groups.Range(func(relativePath string, indexes []int) bool {
		records, err := s.readSegmentRecords(relativePath, segmentPointersAt(pointers, indexes))
		if err != nil {
			resultErr = err
			return false
		}
		for recordIndex, pointerIndex := range indexes {
			out[pointerIndex] = cloneRecord(records[recordIndex])
		}
		return true
	})
	if resultErr != nil {
		return nil, resultErr
	}
	return out, nil
}

func segmentPointersAt(pointers []segmentRecordPointer, indexes []int) []segmentRecordPointer {
	out := collectionlist.NewListWithCapacity[segmentRecordPointer](len(indexes))
	for _, index := range indexes {
		out.Add(pointers[index])
	}
	return out.Values()
}

func (s *StorxLogStore) readPointer(pointer segmentRecordPointer) (Record, error) {
	tp := NewTopicPartition(pointer.Topic, pointer.Partition)
	records, err := s.readSegmentRecords(s.segmentRelativePath(tp, pointer.SegmentID), []segmentRecordPointer{pointer})
	if err != nil {
		return Record{}, err
	}
	record := records[0]
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
