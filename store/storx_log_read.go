package store

import (
	"strconv"
	"time"
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
	indexStart := time.Now()
	pointers := s.recordPointersFrom(topicPartition, offset, maxRecords)
	s.recordReadStage(operation, "index_list", len(pointers), indexStart, nil)
	readStart := time.Now()
	out, err = s.readPointers(pointers)
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
	offset, err := pageCursorOffset(cursor)
	if err != nil {
		return RecordPage{}, err
	}
	pageStart := time.Now()
	pointers, hasMore := s.recordPointersPage(topicPartition, offset, maxRecords)
	s.recordReadStage(operation, "index_page", len(pointers), pageStart, nil)
	readStart := time.Now()
	records, err := s.readPointers(pointers)
	s.recordReadStage(operation, "read_segments", len(records), readStart, err)
	if err != nil {
		return RecordPage{}, err
	}
	nextCursor := ""
	if len(records) > 0 {
		nextCursor = strconv.FormatUint(records[len(records)-1].Offset+1, 10)
	}
	return RecordPage{
		Records:    records,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *StorxLogStore) readPointers(pointers []segmentRecordPointer) ([]Record, error) {
	out := make([]Record, 0, len(pointers))
	for start := 0; start < len(pointers); {
		pointer := pointers[start]
		end := contiguousSegmentPointerRun(pointers, start)
		tp := NewTopicPartition(pointer.Topic, pointer.Partition)
		records, err := s.readSegmentRecords(s.segmentRelativePath(tp, pointer.SegmentID), pointers[start:end])
		if err != nil {
			return nil, err
		}
		out = append(out, records...)
		start = end
	}
	return out, nil
}

func contiguousSegmentPointerRun(pointers []segmentRecordPointer, start int) int {
	first := pointers[start]
	end := start + 1
	for end < len(pointers) {
		next := pointers[end]
		if next.Topic != first.Topic || next.Partition != first.Partition || next.SegmentID != first.SegmentID {
			return end
		}
		end++
	}
	return end
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
	offsets, err := s.PartitionOffsets(topicPartition)
	if err != nil {
		return nil, err
	}
	return offsets.HighWatermark, nil
}

func (s *StorxLogStore) PartitionOffsets(topicPartition TopicPartition) (PartitionOffsetState, error) {
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return PartitionOffsetState{}, err
	}
	s.indexMu.RLock()
	pointers := s.records.GetOrDefault(topicPartition, nil)
	next, ok := s.nextOffsets.Get(topicPartition)
	fallbackNext := nextOffsetFromPointers(pointers)
	recordOffsets := segmentPointerOffsets(pointers)
	s.indexMu.RUnlock()
	if !ok {
		next = fallbackNext
	}
	return partitionOffsetsFromRecordOffsets(topicPartition, next, recordOffsets), nil
}

func (s *StorxLogStore) loadTopicForPartition(topicPartition TopicPartition) (TopicConfig, error) {
	s.indexMu.RLock()
	topic, ok := s.topics.Get(topicPartition.Topic)
	s.indexMu.RUnlock()
	if !ok {
		return TopicConfig{}, E(CodeTopicNotFound, "topic %s not found", topicPartition.Topic)
	}
	if topicPartition.Partition >= topic.Partitions {
		return TopicConfig{}, E(CodePartitionNotFound, "partition %s/%d not found", topicPartition.Topic, topicPartition.Partition)
	}
	return cloneTopic(topic), nil
}

func pageCursorOffset(cursor string) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}
	offset, err := strconv.ParseUint(cursor, 10, 64)
	if err != nil {
		return 0, E(CodeInvalidArgument, "invalid page cursor %q: %v", cursor, err)
	}
	return offset, nil
}
