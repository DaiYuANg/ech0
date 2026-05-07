package store_test

import (
	"path/filepath"
	"testing"

	"github.com/DaiYuANg/ech0/store"
)

func TestMemoryRetentionKeepsOffsetsMonotonic(t *testing.T) {
	st := store.NewMemoryStore()
	topic := store.NewTopicConfig("orders")
	retentionMS := uint64(10)
	topic.RetentionMS = &retentionMS
	requireNoError(t, st.CreateTopic(topic))
	oldMS := uint64(100)
	appendRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old")})
	appendRecord(t, st, store.RecordAppend{Payload: []byte("new")})
	result, err := st.EnforceRetention(120)
	requireNoError(t, err)
	if result.RemovedRecords != 1 {
		t.Fatalf("expected one retained record removed, got %#v", result)
	}
	appended, err := st.Append(store.NewTopicPartition("orders", 0), []byte("after-cleanup"))
	requireNoError(t, err)
	if appended.Offset != 2 {
		t.Fatalf("expected monotonic offset 2 after cleanup, got %d", appended.Offset)
	}
	records, err := st.ReadFrom(store.NewTopicPartition("orders", 0), 0, 10)
	requireNoError(t, err)
	if len(records) != 2 || records[0].Offset != 1 || records[1].Offset != 2 {
		t.Fatalf("unexpected records after retention cleanup: %#v", records)
	}
}

func TestStorxCompactionRemovesStaleKeyVersions(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments", "log.bbolt"))
	defer closeLogStore(t, st)
	topic := store.NewTopicConfig("orders")
	topic.CleanupPolicy = store.TopicCleanupCompact
	topic.CompactionEnabled = true
	requireNoError(t, st.CreateTopic(topic))
	appendKeyedRecord(t, st, "v1")
	appendKeyedRecord(t, st, "v2")
	result, err := st.Compact(store.NowMS(), 2)
	requireNoError(t, err)
	if result.CompactedPartitions != 1 || result.RemovedRecords != 1 {
		t.Fatalf("unexpected compaction result: %#v", result)
	}
	records, err := st.ReadFrom(store.NewTopicPartition("orders", 0), 0, 10)
	requireNoError(t, err)
	if len(records) != 1 || records[0].Offset != 1 || string(records[0].Payload) != "v2" {
		t.Fatalf("unexpected compacted records: %#v", records)
	}
}

func appendRecord(t *testing.T, st *store.MemoryStore, record store.RecordAppend) {
	t.Helper()
	_, err := st.AppendRecord(store.NewTopicPartition("orders", 0), record)
	requireNoError(t, err)
}

func appendKeyedRecord(t *testing.T, st *store.StorxLogStore, payload string) {
	t.Helper()
	_, err := st.AppendRecord(store.NewTopicPartition("orders", 0), store.RecordAppend{Key: []byte("k1"), Payload: []byte(payload)})
	requireNoError(t, err)
}

func openLogStore(t *testing.T, path string) *store.StorxLogStore {
	t.Helper()
	st, err := store.OpenStorxLogStore(path)
	requireNoError(t, err)
	return st
}

func closeLogStore(t *testing.T, st *store.StorxLogStore) {
	t.Helper()
	if closeErr := st.Close(); closeErr != nil {
		t.Logf("close log store: %v", closeErr)
	}
}
