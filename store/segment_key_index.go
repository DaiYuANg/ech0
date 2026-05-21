package store

import (
	"bytes"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

type MessageLogKeyLookupStore interface {
	ReadLatestByKey(topicPartition TopicPartition, key []byte) (Record, bool, error)
}

func (s *StorxLogStore) ReadLatestByKey(topicPartition TopicPartition, key []byte) (Record, bool, error) {
	if len(key) == 0 {
		return Record{}, false, E(CodeInvalidArgument, "record key is required")
	}
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return Record{}, false, err
	}
	if err := s.ensureKeyIndex(topicPartition); err != nil {
		return Record{}, false, err
	}
	s.indexMu.RLock()
	keyPointers := s.keyRecords.GetOrDefault(topicPartition, nil)
	pointer, ok := segmentKeyPointer(keyPointers, key)
	s.indexMu.RUnlock()
	if !ok {
		return Record{}, false, nil
	}
	record, err := s.readPointer(pointer)
	if err != nil {
		return Record{}, false, err
	}
	if !bytes.Equal(record.Key, key) {
		return Record{}, false, E(CodeCodec, "segment key index mismatch for offset %d", pointer.Offset)
	}
	return cloneRecord(record), true, nil
}

func (s *StorxLogStore) ensureKeyIndex(tp TopicPartition) error {
	s.indexMu.RLock()
	ready := s.keyIndexReady.Contains(tp)
	s.indexMu.RUnlock()
	if ready {
		return nil
	}
	lock := s.partitionLock(tp)
	lock.Lock()
	defer lock.Unlock()
	return s.ensureKeyIndexLocked(tp)
}

func (s *StorxLogStore) ensureKeyIndexLocked(tp TopicPartition) error {
	s.indexMu.RLock()
	if s.keyIndexReady.Contains(tp) {
		s.indexMu.RUnlock()
		return nil
	}
	pointers := cloneSegmentPointers(s.records.GetOrDefault(tp, nil))
	s.indexMu.RUnlock()
	records, err := s.readPointers(pointers)
	if err != nil {
		return err
	}
	keyPointers := latestKeyPointersFromRecords(records, pointers)
	s.indexMu.Lock()
	if !s.keyIndexReady.Contains(tp) {
		s.keyRecords.Set(tp, keyPointers)
		s.keyIndexReady.Add(tp)
	}
	s.indexMu.Unlock()
	return nil
}

func (s *StorxLogStore) latestOffsetsByKeyIndexOrRecords(
	tp TopicPartition,
	records []Record,
	pointers []segmentRecordPointer,
) *collectionmapping.Map[string, uint64] {
	s.indexMu.Lock()
	keyPointers, ok := s.keyRecords.Get(tp)
	if !ok || !s.keyIndexReady.Contains(tp) {
		keyPointers = latestKeyPointersFromRecords(records, pointers)
		s.keyRecords.Set(tp, keyPointers)
		s.keyIndexReady.Add(tp)
	}
	latest := keyPointerOffsets(keyPointers)
	s.indexMu.Unlock()
	return latest
}

func (s *StorxLogStore) recordAppendedKeyPointersLocked(
	tp TopicPartition,
	records []Record,
	pointers []segmentRecordPointer,
) {
	if !s.keyIndexReady.Contains(tp) || len(records) == 0 || len(pointers) == 0 {
		return
	}
	keyPointers := s.keyRecords.GetOrDefault(tp, nil)
	if keyPointers == nil {
		keyPointers = collectionmapping.NewMap[string, segmentRecordPointer]()
		s.keyRecords.Set(tp, keyPointers)
	}
	appendKeyPointers(records, pointers, keyPointers)
}

func latestKeyPointersFromRecords(
	records []Record,
	pointers []segmentRecordPointer,
) *collectionmapping.Map[string, segmentRecordPointer] {
	keyPointers := collectionmapping.NewMap[string, segmentRecordPointer]()
	appendKeyPointers(records, pointers, keyPointers)
	return keyPointers
}

func appendKeyPointers(
	records []Record,
	pointers []segmentRecordPointer,
	keyPointers *collectionmapping.Map[string, segmentRecordPointer],
) {
	if keyPointers == nil {
		return
	}
	if len(records) == len(pointers) {
		appendAlignedKeyPointers(records, pointers, keyPointers)
		return
	}
	appendKeyPointersByOffset(records, pointers, keyPointers)
}

func appendAlignedKeyPointers(
	records []Record,
	pointers []segmentRecordPointer,
	keyPointers *collectionmapping.Map[string, segmentRecordPointer],
) {
	for i := range records {
		record := records[i]
		if len(record.Key) == 0 {
			continue
		}
		pointer := pointers[i]
		if pointer.Offset != record.Offset {
			appendKeyPointersByOffset(records, pointers, keyPointers)
			return
		}
		keyPointers.Set(string(record.Key), pointer)
	}
}

func appendKeyPointersByOffset(
	records []Record,
	pointers []segmentRecordPointer,
	keyPointers *collectionmapping.Map[string, segmentRecordPointer],
) {
	byOffset := collectionmapping.NewMapWithCapacity[uint64, segmentRecordPointer](len(pointers))
	for _, pointer := range pointers {
		byOffset.Set(pointer.Offset, pointer)
	}
	for _, record := range records {
		if len(record.Key) == 0 {
			continue
		}
		if pointer, ok := byOffset.Get(record.Offset); ok {
			keyPointers.Set(string(record.Key), pointer)
		}
	}
}

func segmentKeyPointer(
	keyPointers *collectionmapping.Map[string, segmentRecordPointer],
	key []byte,
) (segmentRecordPointer, bool) {
	if keyPointers == nil {
		return segmentRecordPointer{}, false
	}
	return keyPointers.Get(string(key))
}

func keyPointerOffsets(
	keyPointers *collectionmapping.Map[string, segmentRecordPointer],
) *collectionmapping.Map[string, uint64] {
	latest := collectionmapping.NewMap[string, uint64]()
	if keyPointers == nil {
		return latest
	}
	keyPointers.Range(func(key string, pointer segmentRecordPointer) bool {
		latest.Set(key, pointer.Offset)
		return true
	})
	return latest
}
