package queue_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/ech0/queue"
	"github.com/lyonbrown4d/ech0/store"
)

func TestFetchClampsOffsetToLogStartAfterRetention(t *testing.T) {
	st := store.NewMemoryStore()
	runtime := queue.New(st, st)
	topic := store.NewTopicConfig("orders")
	retentionMS := uint64(10)
	topic.RetentionMS = &retentionMS
	requireNoError(t, runtime.CreateTopic(topic))
	oldMS := uint64(100)
	appendRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old-1")})
	appendRecord(t, st, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("old-2")})
	_, err := st.EnforceRetention(context.Background(), 120)
	requireNoError(t, err)

	fromZero := uint64(0)
	poll, err := runtime.Fetch("c1", "orders", 0, &fromZero, 10)
	requireNoError(t, err)
	expectEmptyClampedPoll(t, poll)

	appended, err := runtime.Publish("orders", 0, []byte("after-cleanup"))
	requireNoError(t, err)
	expectRecordOffset(t, appended, 2)
	poll, err = runtime.Fetch("c1", "orders", 0, &fromZero, 10)
	requireNoError(t, err)
	expectAppendedPoll(t, poll)
}

func appendRecord(t *testing.T, st *store.MemoryStore, record store.RecordAppend) {
	t.Helper()
	_, err := st.AppendRecord(store.NewTopicPartition("orders", 0), record)
	requireNoError(t, err)
}

func expectEmptyClampedPoll(t *testing.T, poll store.PollResult) {
	t.Helper()
	if len(poll.Records) != 0 || poll.NextOffset != 2 || poll.LogStartOffset != 2 {
		t.Fatalf("expected empty clamped poll at offset 2, got %#v", poll)
	}
	if poll.LowWatermark != nil || poll.HighWatermark == nil || *poll.HighWatermark != 1 {
		t.Fatalf("unexpected watermarks after empty retention: %#v", poll)
	}
}

func expectRecordOffset(t *testing.T, record store.Record, expected uint64) {
	t.Helper()
	if record.Offset != expected {
		t.Fatalf("expected next append at offset %d, got %d", expected, record.Offset)
	}
}

func expectAppendedPoll(t *testing.T, poll store.PollResult) {
	t.Helper()
	if len(poll.Records) != 1 || poll.Records[0].Offset != 2 || poll.NextOffset != 3 || poll.LogStartOffset != 2 {
		t.Fatalf("unexpected poll after append: %#v", poll)
	}
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
