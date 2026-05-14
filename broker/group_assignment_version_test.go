package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerRejectsStaleConsumerGroupGeneration(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)

	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	if second.Generation <= first.Generation {
		t.Fatalf("expected rebalance to advance generation, first=%#v second=%#v", first, second)
	}

	_, err = b.FetchConsumerGroup(ctx, "workers", "member-1", first.Generation, "orders", 0, nil, 10)
	requireError(t, err)
	requireError(t, b.CommitConsumerGroupOffset(ctx, "workers", "member-1", first.Generation, "orders", 0, 1))
	_, err = b.SeekConsumerGroupOffset(ctx, "workers", "member-1", first.Generation, "orders", 0, 0)
	requireError(t, err)
	_, err = b.PauseConsumerGroup(ctx, "workers", "member-1", first.Generation, "orders", 0)
	requireError(t, err)
}

func TestBrokerRejectsConsumerGroupNonOwner(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	assignment, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	owned := partitionOwnedBy(t, assignment, "member-1")

	_, err = b.FetchConsumerGroup(ctx, "workers", "member-2", assignment.Generation, owned.Topic, owned.Partition, nil, 10)
	requireError(t, err)
	requireError(t, b.CommitConsumerGroupOffset(ctx, "workers", "member-2", assignment.Generation, owned.Topic, owned.Partition, 1))
}

func TestBrokerTransactionalGroupOffsetRequiresCurrentAssignment(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.CommitTransactionOffset(ctx, tx.Identity, broker.TransactionOffsetCommit{
		Group:      "workers",
		MemberID:   "member-1",
		Generation: first.Generation,
		Topic:      "orders",
		Partition:  0,
		NextOffset: 1,
	})
	requireNoError(t, err)
	_, err = b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	state, err := b.ConsumerGroupCommittedOffset(ctx, "workers", "orders", 0)
	requireNoError(t, err)
	if state == nil || state.NextOffset != 1 {
		t.Fatalf("unexpected group offset state: %#v", state)
	}

	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	if second.Generation <= first.Generation {
		t.Fatalf("expected generation to advance, first=%#v second=%#v", first, second)
	}
	staleTx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.CommitTransactionOffset(ctx, staleTx.Identity, broker.TransactionOffsetCommit{
		Group:      "workers",
		MemberID:   "member-1",
		Generation: first.Generation,
		Topic:      "orders",
		Partition:  0,
		NextOffset: 2,
	})
	requireError(t, err)
	_, err = b.AbortTransaction(ctx, staleTx.Identity)
	requireNoError(t, err)
}

func partitionOwnedBy(t *testing.T, assignment store.ConsumerGroupAssignment, memberID string) store.GroupPartitionAssignment {
	t.Helper()
	for _, item := range assignment.Assignments {
		if item.MemberID == memberID {
			return item
		}
	}
	t.Fatalf("assignment has no partition for member %s: %#v", memberID, assignment)
	return store.GroupPartitionAssignment{}
}

func requireError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error")
	}
}
