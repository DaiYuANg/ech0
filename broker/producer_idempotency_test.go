package broker_test

import (
	"context"
	"strings"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerIdempotentBatchDuplicateSequenceReturnsExistingRecords(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	id := broker.ProduceIdempotency{ProducerID: 7, ProducerEpoch: 1, BaseSequence: 10}
	records := []store.RecordAppend{
		store.NewRecordAppend([]byte("m1")),
		store.NewRecordAppend([]byte("m2")),
	}

	first, err := b.PublishBatchIdempotent(ctx, "orders", explicitTestPartition(), records, id)
	requireNoError(t, err)
	duplicate, err := b.PublishBatchIdempotent(ctx, "orders", explicitTestPartition(), []store.RecordAppend{
		store.NewRecordAppend([]byte("m1-retry")),
		store.NewRecordAppend([]byte("m2-retry")),
	}, id)
	requireNoError(t, err)

	if duplicate.Records[0].Offset != first.Records[0].Offset || duplicate.Records[1].Offset != first.Records[1].Offset {
		t.Fatalf("expected duplicate to return original offsets, first=%#v duplicate=%#v", first, duplicate)
	}
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	if len(poll.Records) != 2 || string(poll.Records[0].Payload) != "m1" || string(poll.Records[1].Payload) != "m2" {
		t.Fatalf("expected retry to avoid appending records, got %#v", poll)
	}
}

func TestBrokerIdempotentBatchRejectsOverlappingSequence(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	_, err := b.PublishBatchIdempotent(ctx, "orders", explicitTestPartition(), []store.RecordAppend{
		store.NewRecordAppend([]byte("m1")),
		store.NewRecordAppend([]byte("m2")),
	}, broker.ProduceIdempotency{ProducerID: 7, ProducerEpoch: 1, BaseSequence: 10})
	requireNoError(t, err)

	_, err = b.PublishBatchIdempotent(ctx, "orders", explicitTestPartition(), []store.RecordAppend{
		store.NewRecordAppend([]byte("m2-overlap")),
	}, broker.ProduceIdempotency{ProducerID: 7, ProducerEpoch: 1, BaseSequence: 11})
	if err == nil || !strings.Contains(err.Error(), "does not match published batch") {
		t.Fatalf("expected overlapping sequence error, got %v", err)
	}
}

func TestBrokerIdempotentBatchFencesLowerEpoch(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	_, err := b.PublishBatchIdempotent(ctx, "orders", explicitTestPartition(), []store.RecordAppend{
		store.NewRecordAppend([]byte("m1")),
	}, broker.ProduceIdempotency{ProducerID: 7, ProducerEpoch: 2, BaseSequence: 0})
	requireNoError(t, err)

	_, err = b.PublishBatchIdempotent(ctx, "orders", explicitTestPartition(), []store.RecordAppend{
		store.NewRecordAppend([]byte("stale")),
	}, broker.ProduceIdempotency{ProducerID: 7, ProducerEpoch: 1, BaseSequence: 1})
	if err == nil || !strings.Contains(err.Error(), "fenced") {
		t.Fatalf("expected fenced producer error, got %v", err)
	}
}

func explicitTestPartition() broker.PublishPartitioning {
	return broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}
}
