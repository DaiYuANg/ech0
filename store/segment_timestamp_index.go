package store

import "sort"

type MessageLogTimestampStore interface {
	OffsetForTimestamp(topicPartition TopicPartition, timestampMS uint64) (uint64, *uint64, error)
}

func (s *StorxLogStore) OffsetForTimestamp(topicPartition TopicPartition, timestampMS uint64) (uint64, *uint64, error) {
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return 0, nil, err
	}
	s.indexMu.RLock()
	pointers := cloneSegmentPointers(s.timestampRecords.GetOrDefault(topicPartition, nil))
	if len(pointers) == 0 {
		pointers = cloneSegmentPointers(s.records.GetOrDefault(topicPartition, nil))
		sortSegmentPointersByTimestamp(pointers)
	}
	nextOffset := s.nextOffsets.GetOrDefault(topicPartition, nextOffsetFromPointers(s.records.GetOrDefault(topicPartition, nil)))
	s.indexMu.RUnlock()

	if len(pointers) == 0 {
		return nextOffset, nil, nil
	}
	index := segmentTimestampSearch(pointers, timestampMS)
	if index >= len(pointers) {
		return nextOffset, nil, nil
	}
	record := pointers[index]
	outTimestampMS := record.TimestampMS
	return record.Offset, &outTimestampMS, nil
}

func segmentTimestampSearch(pointers []segmentRecordPointer, timestampMS uint64) int {
	return sort.Search(len(pointers), func(index int) bool {
		return pointers[index].TimestampMS >= timestampMS
	})
}

func sortSegmentPointersByTimestamp(pointers []segmentRecordPointer) []segmentRecordPointer {
	sort.Slice(pointers, func(i, j int) bool {
		if pointers[i].TimestampMS != pointers[j].TimestampMS {
			return pointers[i].TimestampMS < pointers[j].TimestampMS
		}
		return pointers[i].Offset < pointers[j].Offset
	})
	return pointers
}

func insertSegmentTimestampPointerSorted(pointers []segmentRecordPointer, pointer segmentRecordPointer) []segmentRecordPointer {
	index := sort.Search(len(pointers), func(i int) bool {
		if pointers[i].TimestampMS != pointer.TimestampMS {
			return pointers[i].TimestampMS >= pointer.TimestampMS
		}
		return pointers[i].Offset >= pointer.Offset
	})
	if index == len(pointers) {
		return append(pointers, pointer)
	}
	if pointers[index].TimestampMS == pointer.TimestampMS && pointers[index].Offset == pointer.Offset {
		pointers[index] = pointer
		return pointers
	}
	pointers = append(pointers, segmentRecordPointer{})
	copy(pointers[index+1:], pointers[index:])
	pointers[index] = pointer
	return pointers
}
