package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) SaveDLQIndex(entry DLQIndexEntry) error {
	if err := validateDLQIndexEntry(entry); err != nil {
		return err
	}
	key := dlqIndexKey(entry.DLQTopic, entry.DLQPartition, entry.DLQOffset)
	return wrapExternal(s.dlqIndexes.Put(context.Background(), key, cloneDLQIndexEntry(entry)), "save dlq index")
}

func (s *StorxMetadataStore) LoadDLQIndex(dlqTopic string, dlqPartition uint32, dlqOffset uint64) (*DLQIndexEntry, error) {
	key := dlqIndexKey(dlqTopic, dlqPartition, dlqOffset)
	entry, ok, err := s.dlqIndexes.Get(context.Background(), key)
	if err != nil {
		return nil, wrapExternal(err, "load dlq index")
	}
	if !ok {
		var absent *DLQIndexEntry
		return absent, nil
	}
	entry = cloneDLQIndexEntry(entry)
	return &entry, nil
}

func (s *StorxMetadataStore) ListDLQIndexes(filter DLQIndexFilter) ([]DLQIndexEntry, error) {
	out := collectionlist.NewList[DLQIndexEntry]()
	err := s.dlqIndexes.Walk(context.Background(), func(entry bboltx.Entry[string, DLQIndexEntry]) error {
		if dlqIndexMatchesFilter(entry.Value, filter) {
			out.Add(cloneDLQIndexEntry(entry.Value))
		}
		return nil
	})
	if err != nil {
		return nil, wrapExternal(err, "list dlq indexes")
	}
	return sortDLQIndexes(out.Values()), nil
}
