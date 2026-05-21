package store_test

import (
	"os"
	"path/filepath"
	"strings"
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

func TestStorxLogStoreFailsClosedOnTruncatedSegmentDuringIndexRebuild(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	tp := createSegmentRecoveryFixture(t, logPath)
	indexPath := segmentIndexPath(logPath, tp.Topic, tp.Partition)
	segmentPath := segmentFramePath(logPath, tp.Topic, tp.Partition, 0)
	requireNoError(t, os.Remove(indexPath))
	truncateFileByOneByte(t, segmentPath)

	logStore, err := store.OpenStorxLogStore(logPath)
	if logStore != nil {
		closeLogStore(t, logStore)
	}
	if err == nil || !strings.Contains(err.Error(), "truncated segment frame") {
		t.Fatalf("expected truncated segment rebuild error, got %v", err)
	}
}

func TestStorxLogStoreFailsClosedOnTruncatedSegmentIndex(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	tp := createSegmentRecoveryFixture(t, logPath)
	truncateFileByOneByte(t, segmentIndexPath(logPath, tp.Topic, tp.Partition))

	logStore, err := store.OpenStorxLogStore(logPath)
	if logStore != nil {
		closeLogStore(t, logStore)
	}
	if err == nil || !strings.Contains(err.Error(), "read segment index entry") {
		t.Fatalf("expected truncated index error, got %v", err)
	}
}

func TestRepairStorxLogRebuildsMissingSegmentIndex(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	tp := createSegmentRecoveryFixture(t, logPath)
	indexPath := segmentIndexPath(logPath, tp.Topic, tp.Partition)
	requireNoError(t, os.Remove(indexPath))

	result, err := store.RepairStorxLog(logPath, store.StorxLogRepairOptions{})
	requireNoError(t, err)
	if result.MissingIndexes != 1 || result.RebuiltIndexes != 1 {
		t.Fatalf("unexpected repair result: %#v", result)
	}
	if _, statErr := os.Stat(indexPath); statErr != nil {
		t.Fatalf("expected rebuilt segment index to exist: %v", statErr)
	}
	logStore := openLogStore(t, logPath)
	defer closeLogStore(t, logStore)
	records, err := logStore.ReadFrom(tp, 0, 10)
	requireNoError(t, err)
	if len(records) != 2 {
		t.Fatalf("unexpected repaired records: %#v", records)
	}
}

func TestRepairStorxLogDryRunDoesNotWriteMissingIndex(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	tp := createSegmentRecoveryFixture(t, logPath)
	indexPath := segmentIndexPath(logPath, tp.Topic, tp.Partition)
	requireNoError(t, os.Remove(indexPath))

	result, err := store.RepairStorxLog(logPath, store.StorxLogRepairOptions{DryRun: true})
	requireNoError(t, err)
	if result.MissingIndexes != 1 || result.RebuiltIndexes != 0 {
		t.Fatalf("unexpected dry-run repair result: %#v", result)
	}
	if _, statErr := os.Stat(indexPath); !os.IsNotExist(statErr) {
		t.Fatalf("expected dry run to leave index missing, stat err=%v", statErr)
	}
}

func TestRepairStorxLogRebuildsCorruptIndexWhenRequested(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	tp := createSegmentRecoveryFixture(t, logPath)
	indexPath := segmentIndexPath(logPath, tp.Topic, tp.Partition)
	truncateFileByOneByte(t, indexPath)

	result, err := store.RepairStorxLog(logPath, store.StorxLogRepairOptions{RebuildCorruptIndexes: true})
	requireNoError(t, err)
	if result.CorruptIndexes != 1 || result.RebuiltIndexes != 1 || result.BackedUpCorruptIndexFiles != 1 {
		t.Fatalf("unexpected corrupt-index repair result: %#v", result)
	}
	if !corruptIndexBackupExists(t, indexPath) {
		t.Fatalf("expected corrupt index backup next to %s", indexPath)
	}
	logStore := openLogStore(t, logPath)
	defer closeLogStore(t, logStore)
	records, err := logStore.ReadFrom(tp, 0, 10)
	requireNoError(t, err)
	if len(records) != 2 {
		t.Fatalf("unexpected repaired records: %#v", records)
	}
}

func TestRepairStorxLogReportsCorruptIndexWithoutRebuild(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	tp := createSegmentRecoveryFixture(t, logPath)
	truncateFileByOneByte(t, segmentIndexPath(logPath, tp.Topic, tp.Partition))

	result, err := store.RepairStorxLog(logPath, store.StorxLogRepairOptions{})
	if err == nil {
		t.Fatal("expected corrupt index repair error")
	}
	if result.CorruptIndexes != 1 || len(result.Errors) != 1 {
		t.Fatalf("unexpected corrupt-index report: %#v", result)
	}
}

func createSegmentRecoveryFixture(t *testing.T, logPath string) store.TopicPartition {
	t.Helper()
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
	return tp
}

func truncateFileByOneByte(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	requireNoError(t, err)
	if info.Size() <= 1 {
		t.Fatalf("cannot truncate tiny file %s with size %d", path, info.Size())
	}
	requireNoError(t, os.Truncate(path, info.Size()-1))
}

func segmentIndexPath(root, topic string, partition uint32) string {
	return strings.TrimSuffix(segmentFramePath(root, topic, partition, 0), ".seg") + ".idx"
}

func corruptIndexBackupExists(t *testing.T, indexPath string) bool {
	t.Helper()
	matches, err := filepath.Glob(indexPath + ".corrupt.*")
	requireNoError(t, err)
	return len(matches) > 0
}
