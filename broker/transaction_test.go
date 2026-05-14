package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerTransactionCommitControlsReadCommittedVisibility(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)

	hidden, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	if len(hidden.Records) != 0 || hidden.NextOffset != 0 {
		t.Fatalf("expected uncommitted transaction to stay hidden, got %#v", hidden)
	}
	uncommitted, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadUncommitted)
	requireNoError(t, err)
	if len(uncommitted.Records) != 1 || uncommitted.Records[0].Transaction == nil {
		t.Fatalf("expected read_uncommitted to expose transactional metadata, got %#v", uncommitted)
	}

	committed, err := b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	if committed.Status != store.TransactionStatusCommitted {
		t.Fatalf("unexpected commit result: %#v", committed)
	}
	visible, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePollM1(t, visible)
}

func TestBrokerTransactionAbortSkipsReadCommittedRecord(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)
	_, err = b.AbortTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("m2"))
	requireNoError(t, err)

	poll, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "m2" || poll.NextOffset != 3 {
		t.Fatalf("expected aborted record to be skipped, got %#v", poll)
	}
}

func TestBrokerTransactionDuplicateSequenceReturnsExistingRecord(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	first, err := b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)
	duplicate, err := b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1-retry")))
	requireNoError(t, err)

	if duplicate.Record.Offset != first.Record.Offset || duplicate.NextOffset != first.NextOffset {
		t.Fatalf("expected duplicate sequence to return original offsets, first=%#v duplicate=%#v", first, duplicate)
	}
	poll, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadUncommitted)
	requireNoError(t, err)
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "m1" {
		t.Fatalf("expected duplicate sequence to avoid appending a second record, got %#v", poll)
	}
}

func TestBrokerTransactionDuplicateBatchSequenceReturnsExistingRecords(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	first, err := b.PublishTransactionalBatch(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, []store.RecordAppend{
		store.NewRecordAppend([]byte("m1")),
		store.NewRecordAppend([]byte("m2")),
	})
	requireNoError(t, err)
	duplicate, err := b.PublishTransactionalBatch(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, []store.RecordAppend{
		store.NewRecordAppend([]byte("m1-retry")),
		store.NewRecordAppend([]byte("m2-retry")),
	})
	requireNoError(t, err)

	if duplicate.BaseOffset != first.BaseOffset || duplicate.LastOffset != first.LastOffset || duplicate.NextOffset != first.NextOffset {
		t.Fatalf("expected duplicate batch to return original offsets, first=%#v duplicate=%#v", first, duplicate)
	}
	poll, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadUncommitted)
	requireNoError(t, err)
	if len(poll.Records) != 2 || string(poll.Records[0].Payload) != "m1" || string(poll.Records[1].Payload) != "m2" {
		t.Fatalf("expected duplicate batch to avoid appending retry records, got %#v", poll)
	}
}

func TestBrokerTransactionCommitWritesSegmentMarker(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)
	_, err = b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)

	poll, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadUncommitted)
	requireNoError(t, err)
	requireTransactionControlMarker(t, poll, store.TransactionControlCommit)
}

func TestBrokerTransactionAbortWritesSegmentMarker(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)
	_, err = b.AbortTransaction(ctx, tx.Identity)
	requireNoError(t, err)

	poll, err := b.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadUncommitted)
	requireNoError(t, err)
	requireTransactionControlMarker(t, poll, store.TransactionControlAbort)
}

func TestBrokerTransactionCommitsOffsetAtomically(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePollM1(t, poll)

	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.CommitTransactionOffset(ctx, tx.Identity, broker.TransactionOffsetCommit{
		Consumer:   "c1",
		Topic:      "orders",
		Partition:  0,
		NextOffset: poll.NextOffset,
	})
	requireNoError(t, err)
	beforeCommit := fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePollM1(t, beforeCommit)

	_, err = b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	afterCommit := fetchTopic(t, b, "c1", "orders", nil, 10)
	if len(afterCommit.Records) != 0 || afterCommit.NextOffset != 1 {
		t.Fatalf("expected offset to commit with transaction, got %#v", afterCommit)
	}
}

func requireTransactionControlMarker(t *testing.T, poll store.PollResult, controlType store.TransactionControlType) {
	t.Helper()
	if len(poll.Records) != 2 {
		t.Fatalf("expected data record and control marker, got %#v", poll)
	}
	marker := poll.Records[1]
	if len(marker.Payload) != 0 || marker.Transaction == nil || marker.Transaction.ControlType != controlType {
		t.Fatalf("unexpected transaction control marker: %#v", marker)
	}
	if marker.Transaction.Sequence != 1 {
		t.Fatalf("expected marker sequence 1, got %#v", marker.Transaction)
	}
}
