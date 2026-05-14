package ech0_test

import (
	"context"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedConsumerGroupCallbacksTrackRebalanceDelta(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders", ech0.Partitions(2)))
	assignEvents := []ech0.ConsumerGroupRebalance{}
	revokeEvents := []ech0.ConsumerGroupRebalance{}
	group, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, ech0.WithConsumerGroupCallbacks(ech0.ConsumerGroupCallbacks{
		OnAssign: func(_ context.Context, event ech0.ConsumerGroupRebalance) error {
			assignEvents = append(assignEvents, event)
			return nil
		},
		OnRevoke: func(_ context.Context, event ech0.ConsumerGroupRebalance) error {
			revokeEvents = append(revokeEvents, event)
			return nil
		},
	}))
	requireNoError(t, err)
	if len(assignEvents) != 1 || len(assignEvents[0].Assigned) != 2 {
		t.Fatalf("expected initial assignment callback for two partitions, got %#v", assignEvents)
	}

	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"})
	requireNoError(t, err)
	change, err := group.Rebalance(ctx)
	requireNoError(t, err)
	if len(revokeEvents) != 1 || len(revokeEvents[0].Revoked) != 1 {
		t.Fatalf("expected one revoked partition after second member joins, got %#v", revokeEvents)
	}
	if len(change.Assignments) != 1 || group.Assignments().Generation != change.Generation {
		t.Fatalf("unexpected rebalance state: change=%#v current=%#v", change, group.Assignments())
	}
}

func TestEmbeddedConsumerGroupFetchAndAckUseCurrentAssignment(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	group, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"})
	requireNoError(t, err)
	publishEmbedded(ctx, t, b, []byte("m1"))

	fetched, err := group.Fetch(ctx, "orders", 0, ech0.FetchLimit(10))
	requireNoError(t, err)
	if len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected group fetch: %#v", fetched)
	}
	requireNoError(t, group.Ack(ctx, fetched.Messages[0]))
	empty, err := group.Fetch(ctx, "orders", 0, ech0.FetchLimit(10))
	requireNoError(t, err)
	if len(empty.Messages) != 0 || empty.NextOffset != 1 {
		t.Fatalf("expected empty group fetch after ack, got %#v", empty)
	}
}

func TestEmbeddedTransactionCommitsConsumerGroupOffset(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	group, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"})
	requireNoError(t, err)
	publishEmbedded(ctx, t, b, []byte("m1"))
	fetched, err := group.Fetch(ctx, "orders", 0, ech0.FetchLimit(10))
	requireNoError(t, err)

	tx, err := b.BeginTransaction(ctx, "worker-1")
	requireNoError(t, err)
	requireNoError(t, tx.CommitConsumerGroupOffsetWithMetadata(ctx, group, fetched.Messages[0], "group-tx=1"))
	requireNoError(t, tx.Commit(ctx))

	state, err := group.CommittedOffset(ctx, "orders", 0)
	requireNoError(t, err)
	if state == nil || state.NextOffset != 1 || state.Metadata != "group-tx=1" {
		t.Fatalf("unexpected group committed offset: %#v", state)
	}
}
