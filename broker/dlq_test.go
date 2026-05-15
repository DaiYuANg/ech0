package broker_test

import (
	"context"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerDLQQueryAndReplay(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	prepareDLQRecord(ctx, t, b)
	dlqRecord := querySingleDLQRecord(ctx, t, b)
	requireDLQRecordMetadata(t, dlqRecord)
	requireDLQReplay(ctx, t, b, dlqRecord)
}

func prepareDLQRecord(ctx context.Context, t *testing.T, b *broker.Broker) {
	t.Helper()
	createTopic(ctx, t, b, retryTopic("orders", 1))
	publishOrderRecord(ctx, t, b, store.RecordAppend{
		Headers: []store.RecordHeader{{Key: "trace_id", Value: []byte("trace-1")}},
		Payload: []byte("m1"),
	})
	lastErr := "db failed permanently"
	_, err := b.Nack(ctx, "c1", "orders", 0, 0, &lastErr)
	requireNoError(t, err)
	time.Sleep(2 * time.Millisecond)
	_, err = b.ProcessRetryBatch(ctx, "retry-worker", "orders", 0, 10)
	requireNoError(t, err)
}

func querySingleDLQRecord(ctx context.Context, t *testing.T, b *broker.Broker) broker.DLQRecord {
	t.Helper()
	traceID := "trace-1"
	query, err := b.QueryDLQ(ctx, broker.DLQQuery{
		SourceTopic:          "orders",
		Partition:            0,
		MaxRecords:           10,
		ErrorCode:            "retry_exhausted",
		ErrorMessageContains: "failed",
		Headers:              []broker.DLQHeaderFilter{{Key: "trace_id", Value: &traceID}},
	})
	requireNoError(t, err)
	if len(query.Records) != 1 {
		t.Fatalf("expected one dlq record, got %#v", query)
	}
	return query.Records[0]
}

func requireDLQRecordMetadata(t *testing.T, dlqRecord broker.DLQRecord) {
	t.Helper()
	if dlqRecord.OriginalTopic != "orders" || dlqRecord.OriginalOffset != 0 || dlqRecord.RetryCount != 1 {
		t.Fatalf("unexpected dlq metadata: %#v", dlqRecord)
	}
	if headerValue(dlqRecord.Headers, "trace_id") != "trace-1" {
		t.Fatalf("expected original header to be queryable, got %#v", dlqRecord.Headers)
	}
}

func requireDLQReplay(ctx context.Context, t *testing.T, b *broker.Broker, dlqRecord broker.DLQRecord) {
	t.Helper()
	replayed, err := b.ReplayDLQ(ctx, "orders", dlqRecord.DLQPartition, dlqRecord.DLQOffset)
	requireNoError(t, err)
	if replayed.Topic != "orders" || replayed.Partition != 0 || replayed.Offset != 1 {
		t.Fatalf("unexpected replay result: %#v", replayed)
	}
	offset := uint64(1)
	poll, err := b.Fetch(ctx, "replay-reader", "orders", 0, &offset, 1)
	requireNoError(t, err)
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "m1" || headerValue(poll.Records[0].Headers, "x-dlq-error-code") != "" {
		t.Fatalf("unexpected replayed record: %#v", poll.Records)
	}
}

func publishOrderRecord(ctx context.Context, t *testing.T, b *broker.Broker, record store.RecordAppend) broker.ProduceResult {
	t.Helper()
	result, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, record)
	requireNoError(t, err)
	return result
}
