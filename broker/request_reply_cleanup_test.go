package broker_test

import (
	"context"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestRequestReplyCleanupRemovesExpiredReplyCursor(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.RequestReplyCursorTTLMS = 10
	b, err := broker.New(cfg)
	requireNoError(t, err)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("svc.echo"))
	pending, err := b.StartRequest(ctx, "svc.echo", []byte("ping"), broker.RequestOptions{
		InstanceID:   "A1",
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
		Partitioning: broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0},
	})
	requireNoError(t, err)
	requests, err := b.FetchRequests(ctx, "svc-workers", "svc.echo", 0, nil, 1)
	requireNoError(t, err)
	if len(requests.Requests) != 1 {
		t.Fatalf("expected one request, got %#v", requests)
	}
	_, err = b.Reply(ctx, requests.Requests[0], "B1", []byte("pong"))
	requireNoError(t, err)
	_, err = b.AwaitReply(ctx, pending)
	requireNoError(t, err)
	requireNoError(t, b.CommitOffset(ctx, "regular", "svc.echo", 0, 1))

	result, err := b.CleanupRequestReplies(ctx, store.NowMS()+cfg.Broker.RequestReplyCursorTTLMS+1)
	requireNoError(t, err)
	if result.RemovedCursors != 1 {
		t.Fatalf("expected one removed cursor, got %#v", result)
	}
	result, err = b.CleanupRequestReplies(ctx, store.NowMS()+cfg.Broker.RequestReplyCursorTTLMS+1)
	requireNoError(t, err)
	if result.RemovedCursors != 0 {
		t.Fatalf("expected idempotent cleanup, got %#v", result)
	}
	regular, err := b.CommittedOffset(ctx, "regular", "svc.echo", 0)
	requireNoError(t, err)
	if regular == nil || regular.NextOffset != 1 {
		t.Fatalf("expected regular consumer offset to remain, got %#v", regular)
	}
}
