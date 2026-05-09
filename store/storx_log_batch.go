package store

import (
	"errors"
	"os"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

type appendBatchPlan struct {
	records []Record
	writes  []segmentBatchWrite
}

type segmentBatchWrite struct {
	segmentID uint64
	indexes   []int
}

func (s *StorxLogStore) AppendRecordsBatch(topicPartition TopicPartition, records []RecordAppend) ([]Record, error) {
	return s.appendPipeline(topicPartition).append(records)
}

func (s *StorxLogStore) appendRecordsBatchDirect(topicPartition TopicPartition, records []RecordAppend) (out []Record, err error) {
	const operation = "append_batch"
	totalStart := time.Now()
	defer func() {
		s.recordAppendStage(operation, "total", len(records), totalStart, err)
	}()
	if len(records) == 0 {
		return nil, nil
	}

	lockStart := time.Now()
	lock := s.partitionLock(topicPartition)
	lock.Lock()
	s.recordAppendStage(operation, "lock_wait", len(records), lockStart, nil)
	defer lock.Unlock()

	plan, err := s.prepareAppendBatch(operation, topicPartition, records)
	if err != nil {
		return nil, err
	}
	return s.commitAppendBatch(operation, topicPartition, plan)
}

func (s *StorxLogStore) prepareAppendBatch(
	operation string,
	topicPartition TopicPartition,
	appendRecords []RecordAppend,
) (appendBatchPlan, error) {
	topicStart := time.Now()
	topic, loadErr := s.loadTopicForPartition(topicPartition)
	s.recordAppendStage(operation, "load_topic", len(appendRecords), topicStart, loadErr)
	if loadErr != nil {
		return appendBatchPlan{}, loadErr
	}

	offsetStart := time.Now()
	baseOffset := s.nextOffset(topicPartition)
	s.recordAppendStage(operation, "next_offset", len(appendRecords), offsetStart, nil)

	records := make([]Record, len(appendRecords))
	encodeStart := time.Now()
	for index, appendRecord := range appendRecords {
		if len(appendRecord.Payload) > int(topic.MaxMessageBytes) {
			err := E(CodeInvalidArgument, "payload size %d exceeds max_message_bytes %d", len(appendRecord.Payload), topic.MaxMessageBytes)
			s.recordAppendStage(operation, "encode_frame", index+1, encodeStart, err)
			return appendBatchPlan{}, err
		}
		offset, offsetErr := offsetForBatchRecord(baseOffset, index)
		if offsetErr != nil {
			s.recordAppendStage(operation, "encode_frame", index+1, encodeStart, offsetErr)
			return appendBatchPlan{}, offsetErr
		}
		record := newStoredRecord(offset, appendRecord)
		records[index] = record
	}
	s.recordAppendStage(operation, "encode_frame", len(appendRecords), encodeStart, nil)

	writes, writeErr := s.planBatchSegmentWrites(topic, topicPartition, records)
	if writeErr != nil {
		return appendBatchPlan{}, writeErr
	}
	return appendBatchPlan{
		records: records,
		writes:  writes,
	}, nil
}

func offsetForBatchRecord(baseOffset uint64, index int) (uint64, error) {
	indexOffset, err := nonNegativeIntToUint64(index, "batch record index")
	if err != nil {
		return 0, err
	}
	if baseOffset > ^uint64(0)-indexOffset {
		return 0, E(CodeInvalidArgument, "batch offset overflows uint64: base=%d index=%d", baseOffset, index)
	}
	return baseOffset + indexOffset, nil
}

func (s *StorxLogStore) planBatchSegmentWrites(
	topic TopicConfig,
	topicPartition TopicPartition,
	records []Record,
) ([]segmentBatchWrite, error) {
	if len(records) == 0 {
		return nil, nil
	}

	segmentID, currentSize, err := s.initialBatchSegment(topic, topicPartition, records[0].Offset)
	if err != nil {
		return nil, err
	}
	writes := make([]segmentBatchWrite, 0, 1)
	for index, record := range records {
		frameSize := estimatedSegmentFrameSize(record)
		nextSegmentID, nextSize, segmentErr := segmentForBatchFrame(topic, record, segmentID, currentSize, frameSize)
		if segmentErr != nil {
			return nil, segmentErr
		}
		segmentID = nextSegmentID
		currentSize = nextSize
		writes = appendBatchSegmentWrite(writes, segmentID, index)
		currentSize += int64(frameSize)
	}
	return writes, nil
}

func segmentForBatchFrame(
	topic TopicConfig,
	record Record,
	currentSegmentID uint64,
	currentSize int64,
	frameSize int,
) (uint64, int64, error) {
	if topic.SegmentMaxBytes == 0 || currentSize == 0 {
		return currentSegmentID, currentSize, nil
	}
	hasCapacity, err := segmentHasCapacity(currentSize, frameSize, topic.SegmentMaxBytes)
	if err != nil {
		return 0, 0, err
	}
	if hasCapacity {
		return currentSegmentID, currentSize, nil
	}
	return record.Offset, 0, nil
}

func (s *StorxLogStore) initialBatchSegment(topic TopicConfig, topicPartition TopicPartition, firstOffset uint64) (uint64, int64, error) {
	last, ok, err := s.lastRecordPointer(topicPartition)
	if err != nil {
		return 0, 0, err
	}
	if !ok {
		return firstOffset, 0, nil
	}
	if topic.SegmentMaxBytes == 0 {
		return last.SegmentID, 0, nil
	}
	info, statErr := statSegmentFile(s.segmentsDir, s.segmentRelativePath(topicPartition, last.SegmentID))
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return firstOffset, 0, nil
		}
		return 0, 0, wrapExternal(statErr, "stat segment file")
	}
	return last.SegmentID, info.Size(), nil
}

func appendBatchSegmentWrite(writes []segmentBatchWrite, segmentID uint64, index int) []segmentBatchWrite {
	if len(writes) == 0 {
		return append(writes, segmentBatchWrite{segmentID: segmentID, indexes: []int{index}})
	}
	last := len(writes) - 1
	if writes[last].segmentID != segmentID {
		return append(writes, segmentBatchWrite{segmentID: segmentID, indexes: []int{index}})
	}
	writes[last].indexes = append(writes[last].indexes, index)
	return writes
}

func (s *StorxLogStore) commitAppendBatch(
	operation string,
	topicPartition TopicPartition,
	plan appendBatchPlan,
) ([]Record, error) {
	appendStart := time.Now()
	pointers, segmentPaths, appendErr := s.appendBatchFrames(topicPartition, plan)
	s.recordAppendStage(operation, "append_frame", len(plan.records), appendStart, appendErr)
	if appendErr != nil {
		return nil, appendErr
	}

	indexStart := time.Now()
	indexPaths, indexErr := s.appendBatchSegmentIndexes(topicPartition, pointers)
	s.recordAppendStage(operation, "index_set", len(plan.records), indexStart, indexErr)
	if indexErr != nil {
		return nil, wrapExternal(indexErr, "save segment record indexes")
	}

	syncStart := time.Now()
	syncErr := s.syncAppendWrites(segmentPaths, indexPaths)
	s.recordAppendStage(operation, "sync", len(plan.records), syncStart, syncErr)
	if syncErr != nil {
		return nil, syncErr
	}

	nextOffsetStart := time.Now()
	s.recordAppendedPointers(topicPartition, pointers)
	s.recordAppendStage(operation, "next_offset_set", len(plan.records), nextOffsetStart, nil)
	return cloneRecords(plan.records), nil
}

func (s *StorxLogStore) appendBatchSegmentIndexes(topicPartition TopicPartition, pointers []segmentRecordPointer) ([]string, error) {
	paths := collectionlist.NewList[string]()
	start := 0
	for start < len(pointers) {
		segmentID := pointers[start].SegmentID
		end := start + 1
		for end < len(pointers) && pointers[end].SegmentID == segmentID {
			end++
		}
		relativePath := s.segmentRelativePath(topicPartition, segmentID)
		if err := s.appendSegmentIndexPointers(relativePath, pointers[start:end]); err != nil {
			return nil, err
		}
		paths.Add(segmentIndexRelativePath(relativePath))
		start = end
	}
	return paths.Values(), nil
}

func (s *StorxLogStore) appendBatchFrames(topicPartition TopicPartition, plan appendBatchPlan) ([]segmentRecordPointer, []string, error) {
	pointers := make([]segmentRecordPointer, len(plan.records))
	segmentPaths := collectionlist.NewListWithCapacity[string](len(plan.writes))
	for _, write := range plan.writes {
		frame, err := encodeSegmentBatchFrameWithCompression(recordsForBatchWrite(plan.records, write), s.compression)
		if err != nil {
			return nil, nil, err
		}
		result, err := s.appendBatchFrame(topicPartition, write.segmentID, frame)
		if err != nil {
			return nil, nil, err
		}
		segmentPaths.Add(result.relativePath)
		assignBatchFramePointers(topicPartition, plan.records, write, result.positions[0], len(frame), pointers)
	}
	return pointers, segmentPaths.Values(), nil
}

func recordsForBatchWrite(records []Record, write segmentBatchWrite) []Record {
	out := make([]Record, 0, len(write.indexes))
	for _, recordIndex := range write.indexes {
		out = append(out, records[recordIndex])
	}
	return out
}

func (s *StorxLogStore) appendBatchFrame(topicPartition TopicPartition, segmentID uint64, frame []byte) (segmentAppendResult, error) {
	result, err := s.appendFramesToSegment(topicPartition, segmentID, [][]byte{frame})
	if err != nil {
		return segmentAppendResult{}, err
	}
	if len(result.positions) == 0 {
		return segmentAppendResult{}, E(CodeCodec, "append batch frame returned no position")
	}
	return result, nil
}

func assignBatchFramePointers(
	topicPartition TopicPartition,
	records []Record,
	write segmentBatchWrite,
	position int64,
	length int,
	pointers []segmentRecordPointer,
) {
	for _, recordIndex := range write.indexes {
		record := records[recordIndex]
		pointers[recordIndex] = segmentRecordPointer{
			Topic:       topicPartition.Topic,
			Partition:   topicPartition.Partition,
			Offset:      record.Offset,
			SegmentID:   write.segmentID,
			Position:    position,
			Length:      length,
			TimestampMS: record.TimestampMS,
			Attributes:  record.Attributes,
		}
	}
}
