package store_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

func TestStorxStoresPersistLogAndMetadata(t *testing.T) {
	root := t.TempDir()
	logPath := filepath.Join(root, "segments")
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

func TestStorxLogStorePersistsTransactionMetadata(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	logStore := openLogStore(t, logPath)
	topic := store.NewTopicConfig("orders")
	requireNoError(t, logStore.CreateTopic(topic))
	_, err := logStore.AppendRecord(store.NewTopicPartition("orders", 0), store.RecordAppend{
		Transaction: &store.TransactionRecordMetadata{TxID: 7, ProducerID: 7, Sequence: 3},
		Payload:     []byte("m1"),
	})
	requireNoError(t, err)
	closeLogStore(t, logStore)

	logStore = openLogStore(t, logPath)
	defer closeLogStore(t, logStore)
	records, err := logStore.ReadFrom(store.NewTopicPartition("orders", 0), 0, 1)
	requireNoError(t, err)
	if len(records) != 1 || records[0].Transaction == nil || records[0].Transaction.TxID != 7 || records[0].Transaction.Sequence != 3 {
		t.Fatalf("unexpected transaction metadata after reopen: %#v", records)
	}
}

func TestStorxLogStorePersistsSegmentIndexesNextToSegments(t *testing.T) {
	root := t.TempDir()
	logPath := filepath.Join(root, "segments")

	logStore, err := store.OpenStorxLogStore(logPath)
	requireNoError(t, err)
	persistLogRecord(t, logStore)
	requireNoError(t, logStore.Close())

	logStore, err = store.OpenStorxLogStore(logPath)
	requireNoError(t, err)
	defer closeLogStore(t, logStore)
	requirePersistedRecord(t, logStore)

	if _, err := os.Stat(filepath.Join(logPath, "topics.json")); err != nil {
		t.Fatalf("expected segment manifest to exist: %v", err)
	}
	if _, err := os.Stat(filepath.Join(logPath, "segments", "b3JkZXJz", "0", "00000000000000000000.idx")); err != nil {
		t.Fatalf("expected segment index to exist: %v", err)
	}
}

func persistOrderState(t *testing.T, logStore *store.StorxLogStore, metaStore *store.StorxMetadataStore) {
	t.Helper()
	topic := persistLogRecord(t, logStore)
	requireNoError(t, metaStore.SaveTopicConfig(topic))
	requireNoError(t, metaStore.SaveConsumerOffset("c1", store.NewTopicPartition("orders", 0), 1))
}

func persistLogRecord(t *testing.T, logStore *store.StorxLogStore) store.TopicConfig {
	t.Helper()
	topic := store.NewTopicConfig("orders")
	requireNoError(t, logStore.CreateTopic(topic))
	record, err := logStore.Append(store.NewTopicPartition("orders", 0), []byte("m1"))
	requireNoError(t, err)
	if record.Offset != 0 {
		t.Fatalf("unexpected offset: %d", record.Offset)
	}
	return topic
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

func TestStorxLogReadPageUsesCursor(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments"))
	defer closeLogStore(t, st)
	topic := store.NewTopicConfig("orders")
	requireNoError(t, st.CreateTopic(topic))
	for _, payload := range []string{"m1", "m2", "m3"} {
		_, err := st.Append(store.NewTopicPartition("orders", 0), []byte(payload))
		requireNoError(t, err)
	}

	first, err := st.ReadPage(store.NewTopicPartition("orders", 0), "", 2)
	requireNoError(t, err)
	if !first.HasMore || first.NextCursor == "" || len(first.Records) != 2 {
		t.Fatalf("unexpected first page: %#v", first)
	}
	second, err := st.ReadPage(store.NewTopicPartition("orders", 0), first.NextCursor, 2)
	requireNoError(t, err)
	if second.HasMore || len(second.Records) != 1 || string(second.Records[0].Payload) != "m3" {
		t.Fatalf("unexpected second page: %#v", second)
	}
}

func TestStorxLogAppendRecordsBatchPersistsAcrossSegmentRoll(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	st := openLogStore(t, logPath)
	topic := store.NewTopicConfig("orders")
	topic.SegmentMaxBytes = 1
	requireNoError(t, st.CreateTopic(topic))
	tp := store.NewTopicPartition("orders", 0)

	appended, err := st.AppendRecordsBatch(tp, []store.RecordAppend{
		{Payload: []byte("m1")},
		{Payload: []byte("m2")},
		{Payload: []byte("m3")},
	})
	requireNoError(t, err)
	for index, record := range appended {
		if record.Offset != uint64(index) {
			t.Fatalf("unexpected appended offset at %d: %d", index, record.Offset)
		}
	}
	requireNoError(t, st.Close())

	st = openLogStore(t, logPath)
	defer closeLogStore(t, st)
	records, err := st.ReadFrom(tp, 0, 10)
	requireNoError(t, err)
	if len(records) != 3 ||
		string(records[0].Payload) != "m1" ||
		string(records[1].Payload) != "m2" ||
		string(records[2].Payload) != "m3" {
		t.Fatalf("unexpected records after batch append: %#v", records)
	}
	lastOffset, err := st.LastOffset(tp)
	requireNoError(t, err)
	if lastOffset == nil || *lastOffset != 2 {
		t.Fatalf("unexpected last offset after batch append: %#v", lastOffset)
	}
}

func TestStorxLogMmapReadModeReadsSealedSegments(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "segments")
	st, err := store.OpenStorxLogStoreWithOptions(logPath, store.StorxLogOptions{ReadMode: store.SegmentReadModeMmap})
	requireNoError(t, err)
	topic := store.NewTopicConfig("orders")
	topic.SegmentMaxBytes = 1
	requireNoError(t, st.CreateTopic(topic))
	tp := store.NewTopicPartition("orders", 0)
	_, err = st.Append(tp, []byte("m1"))
	requireNoError(t, err)
	_, err = st.Append(tp, []byte("m2"))
	requireNoError(t, err)

	records, err := st.ReadFrom(tp, 0, 10)
	requireNoError(t, err)
	if len(records) != 2 || string(records[0].Payload) != "m1" || string(records[1].Payload) != "m2" {
		t.Fatalf("unexpected records from mmap read mode: %#v", records)
	}
	requireNoError(t, st.Close())

	st, err = store.OpenStorxLogStoreWithOptions(logPath, store.StorxLogOptions{ReadMode: store.SegmentReadModeMmap})
	requireNoError(t, err)
	defer closeLogStore(t, st)
	records, err = st.ReadFrom(tp, 0, 10)
	requireNoError(t, err)
	if len(records) != 2 || string(records[0].Payload) != "m1" || string(records[1].Payload) != "m2" {
		t.Fatalf("unexpected records from reopened mmap read mode: %#v", records)
	}
}

func TestStorxMetadataGroupMembersUseSecondaryIndexAfterReopen(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta", "metadata.bbolt")
	metaStore := openMetadataStore(t, metaPath)
	requireNoError(t, metaStore.SaveGroupMember(store.ConsumerGroupMember{Group: "workers", MemberID: "worker-2", Topics: []string{"orders"}}))
	requireNoError(t, metaStore.SaveGroupMember(store.ConsumerGroupMember{Group: "workers", MemberID: "worker-1", Topics: []string{"orders"}}))
	requireNoError(t, metaStore.SaveGroupMember(store.ConsumerGroupMember{Group: "other", MemberID: "other-1", Topics: []string{"orders"}}))
	requireNoError(t, metaStore.Close())

	metaStore = openMetadataStore(t, metaPath)
	defer closeMetadataStore(t, metaStore)
	members, err := metaStore.ListGroupMembers("workers")
	requireNoError(t, err)
	if len(members) != 2 || members[0].MemberID != "worker-1" || members[1].MemberID != "worker-2" {
		t.Fatalf("unexpected indexed group members: %#v", members)
	}
}

func TestStorxMetadataShardPlacementsPersistAfterReopen(t *testing.T) {
	metaPath := filepath.Join(t.TempDir(), "meta", "metadata.bbolt")
	metaStore := openMetadataStore(t, metaPath)
	requireNoError(t, metaStore.SaveShardPlacement(store.NewShardPlacement("orders", 1, 3)))
	requireNoError(t, metaStore.Close())

	metaStore = openMetadataStore(t, metaPath)
	defer closeMetadataStore(t, metaStore)
	placement, err := metaStore.LoadShardPlacement(store.NewTopicPartition("orders", 1))
	requireNoError(t, err)
	if placement == nil || placement.ShardID != 3 {
		t.Fatalf("unexpected shard placement after reopen: %#v", placement)
	}
	placements, err := metaStore.ListShardPlacements()
	requireNoError(t, err)
	if len(placements) != 1 || placements[0].Topic != "orders" || placements[0].Partition != 1 {
		t.Fatalf("unexpected shard placements: %#v", placements)
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
