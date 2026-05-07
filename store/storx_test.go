package store

import (
	"path/filepath"
	"testing"
)

func TestStorxStoresPersistLogAndMetadata(t *testing.T) {
	root := t.TempDir()
	logPath := filepath.Join(root, "segments", "log.bbolt")
	metaPath := filepath.Join(root, "meta", "metadata.bbolt")

	logStore, err := OpenStorxLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	metaStore, err := OpenStorxMetadataStore(metaPath)
	if err != nil {
		t.Fatal(err)
	}

	topic := NewTopicConfig("orders")
	if err := logStore.CreateTopic(topic); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.SaveTopicConfig(topic); err != nil {
		t.Fatal(err)
	}
	record, err := logStore.Append(NewTopicPartition("orders", 0), []byte("m1"))
	if err != nil {
		t.Fatal(err)
	}
	if record.Offset != 0 {
		t.Fatalf("unexpected offset: %d", record.Offset)
	}
	if err := metaStore.SaveConsumerOffset("c1", NewTopicPartition("orders", 0), 1); err != nil {
		t.Fatal(err)
	}
	if err := logStore.Close(); err != nil {
		t.Fatal(err)
	}
	if err := metaStore.Close(); err != nil {
		t.Fatal(err)
	}

	logStore, err = OpenStorxLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer logStore.Close()
	metaStore, err = OpenStorxMetadataStore(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer metaStore.Close()

	records, err := logStore.ReadFrom(NewTopicPartition("orders", 0), 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || string(records[0].Payload) != "m1" {
		t.Fatalf("unexpected records after reopen: %#v", records)
	}
	offset, err := metaStore.LoadConsumerOffset("c1", NewTopicPartition("orders", 0))
	if err != nil {
		t.Fatal(err)
	}
	if offset == nil || *offset != 1 {
		t.Fatalf("unexpected offset after reopen: %#v", offset)
	}
	topics, err := metaStore.ListTopics()
	if err != nil {
		t.Fatal(err)
	}
	if len(topics) != 1 || topics[0].Name != "orders" {
		t.Fatalf("unexpected topics after reopen: %#v", topics)
	}
}
