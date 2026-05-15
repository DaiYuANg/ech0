package store_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

func TestStorxLogStoreOffsetForTimestamp(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments"))
	defer closeLogStore(t, st)
	requireNoError(t, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	ts10 := uint64(10)
	ts20 := uint64(20)
	ts30 := uint64(30)
	records := []store.RecordAppend{
		{Payload: []byte("m1"), TimestampMS: &ts10},
		{Payload: []byte("m2"), TimestampMS: &ts20},
		{Payload: []byte("m3"), TimestampMS: &ts30},
	}
	_, err := st.AppendRecordsBatch(tp, records)
	requireNoError(t, err)

	offset, ts, err := st.OffsetForTimestamp(tp, 20)
	requireNoError(t, err)
	if offset != 1 || ts == nil || *ts != 20 {
		t.Fatalf("unexpected seek result: offset=%d timestamp=%v", offset, ts)
	}

	offset, ts, err = st.OffsetForTimestamp(tp, 25)
	requireNoError(t, err)
	if offset != 2 || ts == nil || *ts != 30 {
		t.Fatalf("unexpected end-range seek result: offset=%d timestamp=%v", offset, ts)
	}

	offset, ts, err = st.OffsetForTimestamp(tp, 40)
	requireNoError(t, err)
	if offset != 3 || ts != nil {
		t.Fatalf("unexpected no-match seek result: offset=%d timestamp=%v", offset, ts)
	}
}

func TestStorxLogStoreOffsetForTimestampHandlesNonMonotonicTimestamps(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments"))
	defer closeLogStore(t, st)
	requireNoError(t, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	ts80 := uint64(80)
	ts10 := uint64(10)
	ts50 := uint64(50)
	records := []store.RecordAppend{
		{Payload: []byte("m1"), TimestampMS: &ts80},
		{Payload: []byte("m2"), TimestampMS: &ts10},
		{Payload: []byte("m3"), TimestampMS: &ts50},
	}
	_, err := st.AppendRecordsBatch(tp, records)
	requireNoError(t, err)

	offset, ts, err := st.OffsetForTimestamp(tp, 20)
	requireNoError(t, err)
	if offset != 2 || ts == nil || *ts != 50 {
		t.Fatalf("unexpected non-monotonic seek result: offset=%d timestamp=%v", offset, ts)
	}
}

func TestStorxLogStoreOffsetForTimestampAfterIndexRebuild(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	st := openLogStore(t, logPath)
	requireNoError(t, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	ts5 := uint64(5)
	ts15 := uint64(15)
	_, err := st.AppendRecordsBatch(tp, []store.RecordAppend{
		{Payload: []byte("m1"), TimestampMS: &ts5},
		{Payload: []byte("m2"), TimestampMS: &ts15},
	})
	requireNoError(t, err)
	requireNoError(t, st.Close())

	indexDir := filepath.Join(logPath, "segments", "b3JkZXJz", "0")
	matches, err := filepath.Glob(filepath.Join(indexDir, "00000000000000000000*.idx"))
	requireNoError(t, err)
	if len(matches) == 0 {
		requireNoError(t, errors.New("expected timestamp segment index file to remove"))
	}
	for _, path := range matches {
		requireNoError(t, os.Remove(path))
	}

	st = openLogStore(t, logPath)
	defer closeLogStore(t, st)
	offset, ts, err := st.OffsetForTimestamp(tp, 10)
	requireNoError(t, err)
	if offset != 1 || ts == nil || *ts != 15 {
		t.Fatalf("unexpected rebuilt index seek result: offset=%d timestamp=%v", offset, ts)
	}
}
