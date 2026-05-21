package broker_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestFileSinkWritesJSONLinesAndCommitsOffset(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orders.jsonl")
	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	result, err := b.ProcessFileSinkOnce(ctx, broker.FileSinkConfig{
		Name:       "orders-file",
		Topic:      "orders",
		Partition:  0,
		Consumer:   "orders-file",
		Path:       path,
		MaxRecords: 10,
	})
	requireNoError(t, err)
	requireFileSinkResult(t, result, 1, 1)
	requireFileSinkCommitted(ctx, t, b)
	body := readFileSinkBody(t, path)
	if !strings.Contains(body, `"sink":"orders-file"`) || !strings.Contains(body, `"payload":"m1"`) {
		t.Fatalf("unexpected file sink body: %s", body)
	}
}

func TestFileSinkOpenFailureLeavesOffsetUncommitted(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	result, err := b.ProcessFileSinkOnce(ctx, broker.FileSinkConfig{
		Name:       "orders-file",
		Topic:      "orders",
		Partition:  0,
		Consumer:   "orders-file",
		Path:       t.TempDir(),
		MaxRecords: 10,
	})
	if err == nil {
		t.Fatal("expected file sink open failure")
	}
	if result.Delivered != 0 || result.CommittedNextOffset != nil {
		t.Fatalf("unexpected file sink result after failure: %#v", result)
	}
	committed, err := b.CommittedOffset(ctx, "orders-file", "orders", 0)
	requireNoError(t, err)
	if committed != nil {
		t.Fatalf("expected no committed offset after failed file sink, got %#v", committed)
	}
}

func readFileSinkBody(t *testing.T, path string) string {
	t.Helper()
	root, err := os.OpenRoot(filepath.Dir(path))
	requireNoError(t, err)
	file, err := root.Open(filepath.Base(path))
	closeRootErr := root.Close()
	requireNoError(t, err)
	requireNoError(t, closeRootErr)
	body, err := io.ReadAll(file)
	closeErr := file.Close()
	requireNoError(t, err)
	requireNoError(t, closeErr)
	return string(body)
}

func requireFileSinkResult(t *testing.T, result broker.FileSinkResult, delivered int, committed uint64) {
	t.Helper()
	if result.Delivered != delivered || result.CommittedNextOffset == nil || *result.CommittedNextOffset != committed {
		t.Fatalf("unexpected file sink result: %#v", result)
	}
}

func requireFileSinkCommitted(ctx context.Context, t *testing.T, b *broker.Broker) {
	t.Helper()
	committed, err := b.CommittedOffset(ctx, "orders-file", "orders", 0)
	requireNoError(t, err)
	if committed == nil || committed.NextOffset != 1 || committed.Metadata != "file:orders-file" {
		t.Fatalf("unexpected committed offset: %#v", committed)
	}
}
