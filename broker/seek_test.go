package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerSeekOffsetMovesConsumerPosition(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	publishOrder(ctx, t, b, []byte("m2"))
	publishOrder(ctx, t, b, []byte("m3"))

	result, err := b.SeekOffset(ctx, "c1", "orders", 0, 1)
	requireNoError(t, err)
	if result.Offset != 1 || result.Partition != 0 || result.Topic != "orders" {
		t.Fatalf("unexpected seek result: %#v", result)
	}
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePayloads(t, poll, "m2", "m3")
}

func TestBrokerSeekTimestampMovesToFirstMatchingRecord(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishTimestamped(ctx, t, b, "m1", 10)
	publishTimestamped(ctx, t, b, "m2", 20)
	publishTimestamped(ctx, t, b, "m3", 30)

	result, err := b.SeekTimestamp(ctx, "c1", "orders", 0, 20)
	requireNoError(t, err)
	if result.Offset != 1 || result.TimestampMS == nil || *result.TimestampMS != 20 {
		t.Fatalf("unexpected timestamp seek result: %#v", result)
	}
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePayloads(t, poll, "m2", "m3")
}

func TestBrokerSeekTimestampCanMoveToEnd(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishTimestamped(ctx, t, b, "m1", 10)
	publishTimestamped(ctx, t, b, "m2", 20)

	result, err := b.SeekTimestamp(ctx, "c1", "orders", 0, 30)
	requireNoError(t, err)
	if result.Offset != 2 || result.TimestampMS != nil {
		t.Fatalf("unexpected end seek result: %#v", result)
	}
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	if len(poll.Records) != 0 || poll.NextOffset != 2 {
		t.Fatalf("unexpected poll after end seek: %#v", poll)
	}
}

func TestBrokerSeekConsumerGroupOffsetMovesGroupPosition(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	publishOrder(ctx, t, b, []byte("m2"))

	result, err := b.SeekConsumerGroupOffset(ctx, "workers", "member-1", 1, "orders", 0, 1)
	requireNoError(t, err)
	if result.Offset != 1 {
		t.Fatalf("unexpected group seek result: %#v", result)
	}
	poll, err := b.FetchConsumerGroup(ctx, "workers", "member-1", 1, "orders", 0, nil, 10)
	requireNoError(t, err)
	requirePayloads(t, poll, "m2")
}

func publishTimestamped(ctx context.Context, t *testing.T, b *broker.Broker, payload string, timestampMS uint64) {
	t.Helper()
	record := store.NewRecordAppend([]byte(payload))
	record.TimestampMS = &timestampMS
	_, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, record)
	requireNoError(t, err)
}

func requirePayloads(t *testing.T, poll store.PollResult, payloads ...string) {
	t.Helper()
	if len(poll.Records) != len(payloads) {
		t.Fatalf("expected %d records, got %#v", len(payloads), poll)
	}
	for index, payload := range payloads {
		if string(poll.Records[index].Payload) != payload {
			t.Fatalf("expected payload %q at index %d, got %#v", payload, index, poll.Records)
		}
	}
}
