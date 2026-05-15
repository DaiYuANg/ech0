package store_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

func TestStorxLogStoreRebuildsMissingSegmentIndex(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	logStore := openLogStore(t, logPath)
	topic := store.NewTopicConfig("orders")
	requireNoError(t, logStore.CreateTopic(topic))
	tp := store.NewTopicPartition("orders", 0)
	_, err := logStore.AppendRecordsBatch(tp, []store.RecordAppend{
		{Payload: []byte("m1")},
		{Payload: []byte("m2")},
	})
	requireNoError(t, err)
	requireNoError(t, logStore.Close())

	indexPath := filepath.Join(logPath, "segments", "b3JkZXJz", "0", "00000000000000000000.idx")
	requireNoError(t, os.Remove(indexPath))

	logStore = openLogStore(t, logPath)
	defer closeLogStore(t, logStore)
	records, err := logStore.ReadFrom(tp, 0, 10)
	requireNoError(t, err)
	if len(records) != 2 || string(records[0].Payload) != "m1" || string(records[1].Payload) != "m2" {
		t.Fatalf("unexpected rebuilt index records: %#v", records)
	}
	if _, statErr := os.Stat(indexPath); statErr != nil {
		t.Fatalf("expected rebuilt segment index to exist: %v", statErr)
	}
	lastOffset, err := logStore.LastOffset(tp)
	requireNoError(t, err)
	if lastOffset == nil || *lastOffset != 1 {
		t.Fatalf("unexpected last offset after index rebuild: %#v", lastOffset)
	}
}
