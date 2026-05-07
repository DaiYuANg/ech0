package store_test

import (
	"path/filepath"
	"testing"

	"github.com/DaiYuANg/ech0/store"
)

func TestStorxStoresPersistLogAndMetadata(t *testing.T) {
	root := t.TempDir()
	logPath := filepath.Join(root, "segments", "log.bbolt")
	metaPath := filepath.Join(root, "meta", "metadata.bbolt")

	logStore := openLogStore(t, logPath)
	metaStore := openMetadataStore(t, metaPath)
	persistOrderState(t, logStore, metaStore)
	requireNoError(t, logStore.Close())
	requireNoError(t, metaStore.Close())

	logStore = openLogStore(t, logPath)
	defer closeLogStore(t, logStore)
	metaStore = openMetadataStore(t, metaPath)
	defer closeMetadataStore(t, metaStore)

	requirePersistedRecord(t, logStore)
	requirePersistedOffset(t, metaStore)
	requirePersistedTopic(t, metaStore)
}

func persistOrderState(t *testing.T, logStore *store.StorxLogStore, metaStore *store.StorxMetadataStore) {
	t.Helper()
	topic := store.NewTopicConfig("orders")
	requireNoError(t, logStore.CreateTopic(topic))
	requireNoError(t, metaStore.SaveTopicConfig(topic))
	record, err := logStore.Append(store.NewTopicPartition("orders", 0), []byte("m1"))
	requireNoError(t, err)
	if record.Offset != 0 {
		t.Fatalf("unexpected offset: %d", record.Offset)
	}
	requireNoError(t, metaStore.SaveConsumerOffset("c1", store.NewTopicPartition("orders", 0), 1))
}

func requirePersistedRecord(t *testing.T, logStore *store.StorxLogStore) {
	t.Helper()
	records, err := logStore.ReadFrom(store.NewTopicPartition("orders", 0), 0, 10)
	requireNoError(t, err)
	if len(records) != 1 || string(records[0].Payload) != "m1" {
		t.Fatalf("unexpected records after reopen: %#v", records)
	}
}

func requirePersistedOffset(t *testing.T, metaStore *store.StorxMetadataStore) {
	t.Helper()
	offset, err := metaStore.LoadConsumerOffset("c1", store.NewTopicPartition("orders", 0))
	requireNoError(t, err)
	if offset == nil || *offset != 1 {
		t.Fatalf("unexpected offset after reopen: %#v", offset)
	}
}

func requirePersistedTopic(t *testing.T, metaStore *store.StorxMetadataStore) {
	t.Helper()
	topics, err := metaStore.ListTopics()
	requireNoError(t, err)
	if len(topics) != 1 || topics[0].Name != "orders" {
		t.Fatalf("unexpected topics after reopen: %#v", topics)
	}
}

func openMetadataStore(t *testing.T, path string) *store.StorxMetadataStore {
	t.Helper()
	st, err := store.OpenStorxMetadataStore(path)
	requireNoError(t, err)
	return st
}

func closeMetadataStore(t *testing.T, st *store.StorxMetadataStore) {
	t.Helper()
	if closeErr := st.Close(); closeErr != nil {
		t.Logf("close metadata store: %v", closeErr)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
