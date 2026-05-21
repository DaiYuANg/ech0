package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

func TestStorxKeyIndexReadsLatestByKey(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments"))
	defer closeLogStore(t, st)
	requireNoError(t, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	_, err := st.AppendRecordsBatch(tp, []store.RecordAppend{
		{Key: []byte("k1"), Payload: []byte("v1")},
		{Key: []byte("k2"), Payload: []byte("v2")},
		{Key: []byte("k1"), Payload: []byte("v3")},
		{Payload: []byte("unkeyed")},
	})
	requireNoError(t, err)

	expectLatestByKey(t, st, tp, "k1", "v3")
	expectLatestByKey(t, st, tp, "k2", "v2")
	expectMissingLatestByKey(t, st, tp, "missing")

	_, err = st.AppendRecord(tp, store.RecordAppend{Key: []byte("k1"), Payload: []byte("v4")})
	requireNoError(t, err)
	expectLatestByKey(t, st, tp, "k1", "v4")
}

func TestStorxKeyIndexRebuildsAfterReopen(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	st := openLogStore(t, logPath)
	requireNoError(t, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	_, err := st.AppendRecordsBatch(tp, []store.RecordAppend{
		{Key: []byte("k1"), Payload: []byte("v1")},
		{Key: []byte("k1"), Payload: []byte("v2")},
	})
	requireNoError(t, err)
	requireNoError(t, st.Close())

	st = openLogStore(t, logPath)
	defer closeLogStore(t, st)
	expectLatestByKey(t, st, tp, "k1", "v2")
}

func TestStorxCompactionUsesRebuiltKeyIndex(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	st := openLogStore(t, logPath)
	topic := store.NewTopicConfig("orders")
	topic.CleanupPolicy = store.TopicCleanupCompact
	topic.CompactionEnabled = true
	requireNoError(t, st.CreateTopic(topic))
	tp := store.NewTopicPartition("orders", 0)
	_, err := st.AppendRecordsBatch(tp, []store.RecordAppend{
		{Key: []byte("k1"), Payload: []byte("v1")},
		{Key: []byte("k2"), Payload: []byte("v2")},
		{Key: []byte("k1"), Payload: []byte("v3")},
	})
	requireNoError(t, err)
	requireNoError(t, st.Close())

	st = openLogStore(t, logPath)
	defer closeLogStore(t, st)
	result, err := st.Compact(context.Background(), store.NowMS(), 2)
	requireNoError(t, err)
	if result.CompactedPartitions != 1 || result.RemovedRecords != 1 {
		t.Fatalf("unexpected compaction result: %#v", result)
	}
	records, err := st.ReadFrom(tp, 0, 10)
	requireNoError(t, err)
	if len(records) != 2 || string(records[0].Payload) != "v2" || string(records[1].Payload) != "v3" {
		t.Fatalf("unexpected compacted records: %#v", records)
	}
	expectLatestByKey(t, st, tp, "k1", "v3")
}

func TestStorxKeyIndexRemovesExpiredTombstone(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments"))
	defer closeLogStore(t, st)
	topic := store.NewTopicConfig("orders")
	topic.CleanupPolicy = store.TopicCleanupCompact
	topic.CompactionEnabled = true
	requireNoError(t, st.CreateTopic(topic))
	tp := store.NewTopicPartition("orders", 0)
	_, err := st.AppendRecordsBatch(tp, []store.RecordAppend{
		{Key: []byte("k1"), Payload: []byte("v1")},
		{Key: []byte("k1"), Attributes: store.RecordAttributeTombstone},
	})
	requireNoError(t, err)

	expectLatestByKey(t, st, tp, "k1", "")
	_, err = st.Compact(context.Background(), store.NowMS(), 2)
	requireNoError(t, err)
	expectMissingLatestByKey(t, st, tp, "k1")
}

func expectLatestByKey(t *testing.T, st *store.StorxLogStore, tp store.TopicPartition, key, payload string) {
	t.Helper()
	record, ok, err := st.ReadLatestByKey(tp, []byte(key))
	requireNoError(t, err)
	if !ok || string(record.Payload) != payload {
		t.Fatalf("unexpected latest record for %q: %#v", key, record)
	}
}

func expectMissingLatestByKey(t *testing.T, st *store.StorxLogStore, tp store.TopicPartition, key string) {
	t.Helper()
	record, ok, err := st.ReadLatestByKey(tp, []byte(key))
	requireNoError(t, err)
	if ok {
		t.Fatalf("expected no latest record for %q, got %#v", key, record)
	}
}
