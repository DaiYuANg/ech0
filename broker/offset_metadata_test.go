package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerCommitOffsetStoresMetadata(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)

	requireNoError(t, b.CommitOffsetWithMetadata(ctx, "c1", "orders", 0, poll.NextOffset, "checkpoint=42"))
	state, err := b.CommittedOffset(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	if state == nil || state.NextOffset != 1 || state.Metadata != "checkpoint=42" || state.Topic != "orders" {
		t.Fatalf("unexpected committed offset state: %#v", state)
	}
}

func TestBrokerConsumerGroupCommitOffsetStoresMetadata(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	assignment, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	if assignment.Generation == 0 {
		t.Fatalf("unexpected group assignment: %#v", assignment)
	}
	poll, err := b.FetchConsumerGroup(ctx, "workers", "member-1", assignment.Generation, "orders", 0, nil, 10)
	requireNoError(t, err)
	requirePollM1(t, poll)

	requireNoError(t, b.CommitConsumerGroupOffsetWithMetadata(ctx, "workers", "member-1", assignment.Generation, "orders", 0, poll.NextOffset, "group-checkpoint=7"))
	state, err := b.ConsumerGroupCommittedOffset(ctx, "workers", "orders", 0)
	requireNoError(t, err)
	if state == nil || state.Consumer != "workers" || state.NextOffset != 1 || state.Metadata != "group-checkpoint=7" {
		t.Fatalf("unexpected group offset state: %#v", state)
	}
}

func TestBrokerTransactionCommitOffsetStoresMetadata(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.CommitTransactionOffset(ctx, tx.Identity, broker.TransactionOffsetCommit{
		Consumer:   "c1",
		Topic:      "orders",
		Partition:  0,
		NextOffset: poll.NextOffset,
		Metadata:   "tx-checkpoint=9",
	})
	requireNoError(t, err)
	_, err = b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)

	state, err := b.CommittedOffset(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	if state == nil || state.NextOffset != 1 || state.Metadata != "tx-checkpoint=9" {
		t.Fatalf("unexpected transaction offset state: %#v", state)
	}
}
