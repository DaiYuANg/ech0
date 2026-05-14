package store_test

import (
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

func TestStorxMetadataPersistsConsumerPause(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta", "metadata.bbolt")
	metaStore := openMetadataStore(t, metaPath)
	requireNoError(t, metaStore.SaveConsumerPause(store.ConsumerPauseState{
		Consumer:    "c1",
		Topic:       "orders",
		Partition:   0,
		Paused:      true,
		UpdatedAtMS: 100,
	}))
	requireNoError(t, metaStore.Close())

	metaStore = openMetadataStore(t, metaPath)
	defer closeMetadataStore(t, metaStore)
	state, err := metaStore.LoadConsumerPause("c1", store.NewTopicPartition("orders", 0))
	requireNoError(t, err)
	if state == nil || !state.Paused || state.UpdatedAtMS != 100 {
		t.Fatalf("unexpected persisted consumer pause: %#v", state)
	}
	requireNoError(t, metaStore.DeleteConsumerPause("c1", store.NewTopicPartition("orders", 0)))
	state, err = metaStore.LoadConsumerPause("c1", store.NewTopicPartition("orders", 0))
	requireNoError(t, err)
	if state != nil {
		t.Fatalf("expected consumer pause to be deleted, got %#v", state)
	}
}
