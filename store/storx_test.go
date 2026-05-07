//nolint:testpackage // Same-package tests use unexported store helpers.
package store

import (
	"path/filepath"
	"testing"
)

//nolint:cyclop,gocyclo,gocognit,funlen // This persistence test keeps reopen assertions in one flow.
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
	if createErr := logStore.CreateTopic(topic); createErr != nil {
		t.Fatal(createErr)
	}
	if saveTopicErr := metaStore.SaveTopicConfig(topic); saveTopicErr != nil {
		t.Fatal(saveTopicErr)
	}
	record, err := logStore.Append(NewTopicPartition("orders", 0), []byte("m1"))
	if err != nil {
		t.Fatal(err)
	}
	if record.Offset != 0 {
		t.Fatalf("unexpected offset: %d", record.Offset)
	}
	if saveOffsetErr := metaStore.SaveConsumerOffset("c1", NewTopicPartition("orders", 0), 1); saveOffsetErr != nil {
		t.Fatal(saveOffsetErr)
	}
	if closeLogErr := logStore.Close(); closeLogErr != nil {
		t.Fatal(closeLogErr)
	}
	if closeMetaErr := metaStore.Close(); closeMetaErr != nil {
		t.Fatal(closeMetaErr)
	}

	logStore, err = OpenStorxLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := logStore.Close(); closeErr != nil {
			t.Logf("close log store: %v", closeErr)
		}
	}()
	metaStore, err = OpenStorxMetadataStore(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := metaStore.Close(); closeErr != nil {
			t.Logf("close metadata store: %v", closeErr)
		}
	}()

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
