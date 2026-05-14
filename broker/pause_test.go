package broker_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerPauseResumeConsumerPartition(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	paused, err := b.PauseConsumer(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	if !paused.Paused || paused.Topic != "orders" || paused.Partition != 0 {
		t.Fatalf("unexpected pause result: %#v", paused)
	}
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	if len(poll.Records) != 0 || poll.NextOffset != 0 {
		t.Fatalf("expected paused fetch to stay empty at offset 0, got %#v", poll)
	}

	resumed, err := b.ResumeConsumer(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	if resumed.Paused {
		t.Fatalf("unexpected resume result: %#v", resumed)
	}
	poll = fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePayloads(t, poll, "m1")
}

func TestBrokerPauseResumeConsumerGroupPartition(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	assignment, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)

	_, err = b.PauseConsumerGroup(ctx, "workers", "member-1", assignment.Generation, "orders", 0)
	requireNoError(t, err)
	poll, err := b.FetchConsumerGroup(ctx, "workers", "member-1", assignment.Generation, "orders", 0, nil, 10)
	requireNoError(t, err)
	if len(poll.Records) != 0 || poll.NextOffset != 0 {
		t.Fatalf("expected paused group fetch to stay empty at offset 0, got %#v", poll)
	}

	_, err = b.ResumeConsumerGroup(ctx, "workers", "member-1", assignment.Generation, "orders", 0)
	requireNoError(t, err)
	poll, err = b.FetchConsumerGroup(ctx, "workers", "member-1", assignment.Generation, "orders", 0, nil, 10)
	requireNoError(t, err)
	requirePayloads(t, poll, "m1")
}
