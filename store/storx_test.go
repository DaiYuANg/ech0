package store_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/DaiYuANg/ech0/store"
	storxobserver "github.com/arcgolabs/storx/observer"
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

func TestStorxLogReadPageUsesCursor(t *testing.T) {
	st := openLogStore(t, filepath.Join(t.TempDir(), "segments", "log.bbolt"))
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
	logPath := filepath.Join(t.TempDir(), "segments", "log.bbolt")
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

func TestStorxObserverReceivesStorageEvents(t *testing.T) {
	events := 0
	obs := storxobserver.ObserverFunc(func(_ context.Context, event storxobserver.Event) {
		if event.Engine == "badger" && event.TargetType == "namespace" {
			events++
		}
	})
	st, err := store.OpenStorxLogStoreWithOptions(
		filepath.Join(t.TempDir(), "segments", "log.bbolt"),
		store.StorxLogOptions{Observers: []store.StorxObserver{obs}},
	)
	requireNoError(t, err)
	defer closeLogStore(t, st)
	requireNoError(t, st.CreateTopic(store.NewTopicConfig("orders")))
	_, err = st.Append(store.NewTopicPartition("orders", 0), []byte("m1"))
	requireNoError(t, err)
	if events == 0 {
		t.Fatal("expected storx observer to receive events")
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
