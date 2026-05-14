package store_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

func TestMemorySnapshotRoundTripsOffsetMetadata(t *testing.T) {
	st := store.NewMemoryStore()
	requireNoError(t, st.SaveConsumerOffsetState(store.ConsumerOffsetState{
		Consumer:    "c1",
		Topic:       "orders",
		Partition:   0,
		NextOffset:  7,
		Metadata:    "checkpoint=42",
		UpdatedAtMS: 100,
	}))
	snapshot, err := st.Snapshot()
	requireNoError(t, err)
	raw, err := json.Marshal(snapshot)
	requireNoError(t, err)
	var decoded store.Snapshot
	requireNoError(t, json.Unmarshal(raw, &decoded))

	restored := store.NewMemoryStore()
	requireNoError(t, restored.Restore(decoded))
	state, err := restored.LoadConsumerOffsetState("c1", store.NewTopicPartition("orders", 0))
	requireNoError(t, err)
	if state == nil || state.NextOffset != 7 || state.Metadata != "checkpoint=42" || state.UpdatedAtMS != 100 {
		t.Fatalf("unexpected restored offset state: %#v", state)
	}
}

func TestStorxMetadataPersistsOffsetMetadata(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta", "metadata.bbolt")
	metaStore := openMetadataStore(t, metaPath)
	requireNoError(t, metaStore.SaveConsumerOffsetState(store.ConsumerOffsetState{
		Consumer:    "c1",
		Topic:       "orders",
		Partition:   0,
		NextOffset:  7,
		Metadata:    "checkpoint=42",
		UpdatedAtMS: 100,
	}))
	requireNoError(t, metaStore.Close())

	metaStore = openMetadataStore(t, metaPath)
	defer closeMetadataStore(t, metaStore)
	state, err := metaStore.LoadConsumerOffsetState("c1", store.NewTopicPartition("orders", 0))
	requireNoError(t, err)
	if state == nil || state.NextOffset != 7 || state.Metadata != "checkpoint=42" || state.UpdatedAtMS != 100 {
		t.Fatalf("unexpected persisted offset state: %#v", state)
	}
	offset, err := metaStore.LoadConsumerOffset("c1", store.NewTopicPartition("orders", 0))
	requireNoError(t, err)
	if offset == nil || *offset != 7 {
		t.Fatalf("unexpected legacy offset view: %#v", offset)
	}
}
