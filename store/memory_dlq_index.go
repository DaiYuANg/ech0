package store

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (s *MemoryStore) SaveDLQIndex(entry DLQIndexEntry) error {
	if err := validateDLQIndexEntry(entry); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlqIndexes.Set(dlqIndexKey(entry.DLQTopic, entry.DLQPartition, entry.DLQOffset), cloneDLQIndexEntry(entry))
	return nil
}

func (s *MemoryStore) LoadDLQIndex(dlqTopic string, dlqPartition uint32, dlqOffset uint64) (*DLQIndexEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.dlqIndexes.Get(dlqIndexKey(dlqTopic, dlqPartition, dlqOffset))
	if !ok {
		var absent *DLQIndexEntry
		return absent, nil
	}
	entry = cloneDLQIndexEntry(entry)
	return &entry, nil
}

func (s *MemoryStore) ListDLQIndexes(filter DLQIndexFilter) ([]DLQIndexEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := collectionlist.NewList[DLQIndexEntry]()
	s.dlqIndexes.Range(func(_ string, entry DLQIndexEntry) bool {
		if dlqIndexMatchesFilter(entry, filter) {
			out.Add(cloneDLQIndexEntry(entry))
		}
		return true
	})
	return sortDLQIndexes(out.Values()), nil
}

func validateDLQIndexEntry(entry DLQIndexEntry) error {
	if entry.DLQTopic == "" {
		return E(CodeInvalidArgument, "dlq topic is required")
	}
	if entry.OriginalTopic == "" {
		return E(CodeInvalidArgument, "original topic is required")
	}
	return nil
}

func restoreMemoryDLQIndexes(snapshotIndexes collectionlist.List[DLQIndexEntry]) *collectionmapping.Map[string, DLQIndexEntry] {
	indexes := collectionmapping.NewMapWithCapacity[string, DLQIndexEntry](snapshotIndexes.Len())
	snapshotIndexes.Range(func(_ int, entry DLQIndexEntry) bool {
		if validateDLQIndexEntry(entry) == nil {
			indexes.Set(dlqIndexKey(entry.DLQTopic, entry.DLQPartition, entry.DLQOffset), cloneDLQIndexEntry(entry))
		}
		return true
	})
	return indexes
}
