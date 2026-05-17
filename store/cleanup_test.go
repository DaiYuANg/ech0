package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
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
	result, err := st.EnforceRetention(context.Background(), 120)
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

func TestMemoryRetentionAdvancesLogStartWhenPartitionEmptied(t *testing.T) {
	st := store.NewMemoryStore()
	topic := store.NewTopicConfig("orders")
	retentionMS := uint64(10)
	topic.RetentionMS = &retentionMS
	requireNoError(t, st.CreateTopic(topic))
	oldMS := uint64(100)
	appendRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old-1")})
	appendRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old-2")})
	result, err := st.EnforceRetention(context.Background(), 120)
	requireNoError(t, err)
	if result.RemovedRecords != 2 {
		t.Fatalf("expected two removed records, got %#v", result)
	}
	high := uint64(1)
	expectPartitionOffsets(t, st, store.NewTopicPartition("orders", 0), 2, nil, &high, 2, 0)
	appended, err := st.Append(store.NewTopicPartition("orders", 0), []byte("after-cleanup"))
	requireNoError(t, err)
	if appended.Offset != 2 {
		t.Fatalf("expected offset 2 after empty retention cleanup, got %d", appended.Offset)
	}
	low := uint64(2)
	high = 2
	expectPartitionOffsets(t, st, store.NewTopicPartition("orders", 0), 2, &low, &high, 3, 1)
}

func TestMemoryRetentionMaxBytesAdvancesLowWatermark(t *testing.T) {
	st := store.NewMemoryStore()
	topic := store.NewTopicConfig("orders")
	topic.RetentionMaxBytes = 40
	requireNoError(t, st.CreateTopic(topic))
	appendRecord(t, st, store.RecordAppend{Payload: []byte("0123456789")})
	appendRecord(t, st, store.RecordAppend{Payload: []byte("abcdefghij")})
	result, err := st.EnforceRetention(context.Background(), store.NowMS())
	requireNoError(t, err)
	if result.RemovedRecords != 1 {
		t.Fatalf("expected one size-retained record removed, got %#v", result)
	}
	low := uint64(1)
	high := uint64(1)
	expectPartitionOffsets(t, st, store.NewTopicPartition("orders", 0), 1, &low, &high, 2, 1)
}

func TestMemoryMessageTTLExpiresIndependentOfCleanupPolicy(t *testing.T) {
	st := store.NewMemoryStore()
	topic := store.NewTopicConfig("orders")
	topic.CleanupPolicy = store.TopicCleanupCompact
	ttlMS := uint64(10)
	topic.MessageTTLMS = &ttlMS
	requireNoError(t, st.CreateTopic(topic))
	oldMS := uint64(100)
	appendRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old")})

	result, err := st.EnforceRetention(context.Background(), 110)
	requireNoError(t, err)
	if result.RemovedRecords != 1 {
		t.Fatalf("expected one expired message to be removed, got %#v", result)
	}
	records, err := st.ReadFrom(store.NewTopicPartition("orders", 0), 0, 10)
	requireNoError(t, err)
	if len(records) != 0 {
		t.Fatalf("expected expired message to be removed, got %#v", records)
	}
}

func TestMemoryExplicitMessageExpiry(t *testing.T) {
	st := store.NewMemoryStore()
	topic := store.NewTopicConfig("orders")
	requireNoError(t, st.CreateTopic(topic))
	expiresAt := uint64(50)
	appendRecord(t, st, store.RecordAppend{ExpiresAtMS: &expiresAt, Payload: []byte("old")})

	result, err := st.EnforceRetention(context.Background(), 50)
	requireNoError(t, err)
	if result.RemovedRecords != 1 {
		t.Fatalf("expected one explicitly expired message to be removed, got %#v", result)
	}
}

func TestStorxRetentionRestoresWatermarksAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segments")
	st := openLogStore(t, path)
	topic := store.NewTopicConfig("orders")
	retentionMS := uint64(10)
	topic.RetentionMS = &retentionMS
	requireNoError(t, st.CreateTopic(topic))
	oldMS := uint64(100)
	appendStorxRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old-1")})
	appendStorxRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old-2")})
	result, err := st.EnforceRetention(context.Background(), 120)
	requireNoError(t, err)
	if result.RemovedRecords != 2 {
		t.Fatalf("expected two removed segment records, got %#v", result)
	}
	high := uint64(1)
	expectPartitionOffsets(t, st, store.NewTopicPartition("orders", 0), 2, nil, &high, 2, 0)
	requireNoError(t, st.Close())

	st = openLogStore(t, path)
	defer closeLogStore(t, st)
	expectPartitionOffsets(t, st, store.NewTopicPartition("orders", 0), 2, nil, &high, 2, 0)
	appended, err := st.Append(store.NewTopicPartition("orders", 0), []byte("after-cleanup"))
	requireNoError(t, err)
	if appended.Offset != 2 {
		t.Fatalf("expected restored segment next offset 2, got %d", appended.Offset)
	}
	low := uint64(2)
	high = 2
	expectPartitionOffsets(t, st, store.NewTopicPartition("orders", 0), 2, &low, &high, 3, 1)
}

func TestStorxCompactionRemovesStaleKeyVersions(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments"))
	defer closeLogStore(t, st)
	topic := store.NewTopicConfig("orders")
	topic.CleanupPolicy = store.TopicCleanupCompact
	topic.CompactionEnabled = true
	requireNoError(t, st.CreateTopic(topic))
	appendKeyedRecord(t, st, "v1")
	appendKeyedRecord(t, st, "v2")
	result, err := st.Compact(context.Background(), store.NowMS(), 2)
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

func appendStorxRecord(t *testing.T, st *store.StorxLogStore, record store.RecordAppend) {
	t.Helper()
	_, err := st.AppendRecord(store.NewTopicPartition("orders", 0), record)
	requireNoError(t, err)
}

func expectPartitionOffsets(
	t *testing.T,
	st interface {
		PartitionOffsets(store.TopicPartition) (store.PartitionOffsetState, error)
	},
	tp store.TopicPartition,
	logStart uint64,
	low *uint64,
	high *uint64,
	next uint64,
	retained uint64,
) {
	t.Helper()
	offsets, err := st.PartitionOffsets(tp)
	requireNoError(t, err)
	if offsets.LogStartOffset != logStart || offsets.NextOffset != next || offsets.RetainedRecords != retained {
		t.Fatalf("unexpected offsets: %#v", offsets)
	}
	expectOptionalOffset(t, "low watermark", offsets.LowWatermark, low)
	expectOptionalOffset(t, "high watermark", offsets.HighWatermark, high)
}

func expectOptionalOffset(t *testing.T, name string, actual, expected *uint64) {
	t.Helper()
	if actual == nil || expected == nil {
		if actual != nil || expected != nil {
			t.Fatalf("unexpected %s: got %v want %v", name, actual, expected)
		}
		return
	}
	if *actual != *expected {
		t.Fatalf("unexpected %s: got %d want %d", name, *actual, *expected)
	}
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
