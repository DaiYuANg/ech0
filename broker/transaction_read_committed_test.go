package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerReadCommittedStopsAtOpenTransactionBoundary(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m2")))
	requireNoError(t, err)
	publishOrder(ctx, t, b, []byte("m3"))

	blocked, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePayloads(t, blocked, "m1")
	if blocked.NextOffset != 1 {
		t.Fatalf("expected read_committed to stop at open transaction offset 1, got %#v", blocked)
	}

	_, err = b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	offset := uint64(1)
	visible, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, &offset, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePayloads(t, visible, "m2", "m3")
}

func TestBrokerReadCommittedSkipsControlMarkersAcrossSmallBatches(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)
	_, err = b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	publishOrder(ctx, t, b, []byte("m2"))

	first, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 1, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePayloads(t, first, "m1")
	if first.NextOffset != 1 {
		t.Fatalf("expected first small read to stop after data record, got %#v", first)
	}
	secondOffset := first.NextOffset
	second, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, &secondOffset, 1, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePayloads(t, second, "m2")
	if second.NextOffset != 3 {
		t.Fatalf("expected second small read to skip commit marker, got %#v", second)
	}
}

func TestBrokerReadCommittedSkipsAbortedBatch(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalBatch(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, []store.RecordAppend{
		store.NewRecordAppend([]byte("m1")),
		store.NewRecordAppend([]byte("m2")),
	})
	requireNoError(t, err)
	_, err = b.AbortTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	publishOrder(ctx, t, b, []byte("m3"))

	poll, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePayloads(t, poll, "m3")
	if poll.NextOffset != 4 {
		t.Fatalf("expected read_committed to skip aborted batch and marker, got %#v", poll)
	}
}
