package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestTopicPriorityPolicyDefaultsAndValidatesProduce(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	topic := store.NewTopicConfig("orders")
	topic.PriorityPolicy = store.TopicPriorityPolicy{Enabled: true, Min: 1, Max: 5, Default: 3}
	createTopic(ctx, t, b, topic)

	first, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{}, store.NewRecordAppend([]byte("default")))
	requireNoError(t, err)
	if got := headerValue(first.Record.Headers, broker.PriorityHeader); got != "3" {
		t.Fatalf("default priority header = %q, want 3", got)
	}

	record := store.NewRecordAppend([]byte("explicit"))
	record.Headers = []store.RecordHeader{{Key: broker.PriorityHeader, Value: []byte("5")}}
	second, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{}, record)
	requireNoError(t, err)
	if got := headerValue(second.Record.Headers, broker.PriorityHeader); got != "5" {
		t.Fatalf("explicit priority header = %q, want 5", got)
	}

	invalid := store.NewRecordAppend([]byte("invalid"))
	invalid.Headers = []store.RecordHeader{{Key: broker.PriorityHeader, Value: []byte("9")}}
	if _, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{}, invalid); err == nil {
		t.Fatal("expected out-of-range priority to be rejected")
	}
}

func TestTopicPriorityPolicyAppliesToCoalescedBatches(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	topic := store.NewTopicConfig("orders")
	topic.PriorityPolicy = store.TopicPriorityPolicy{Enabled: true, Min: 0, Max: 9, Default: 4}
	createTopic(ctx, t, b, topic)

	result, err := b.PublishBatch(ctx, "orders", broker.PublishPartitioning{}, []store.RecordAppend{
		store.NewRecordAppend([]byte("a")),
		store.NewRecordAppend([]byte("b")),
	})
	requireNoError(t, err)
	if len(result.Records) != 2 {
		t.Fatalf("records = %d, want 2", len(result.Records))
	}
	for index := range result.Records {
		if got := headerValue(result.Records[index].Headers, broker.PriorityHeader); got != "4" {
			t.Fatalf("record %d priority = %q, want 4", index, got)
		}
	}
}

func TestTopicPriorityPolicyOrdersFetchBatchAndAcksOutOfOrder(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	topic := store.NewTopicConfig("orders")
	topic.PriorityPolicy = store.TopicPriorityPolicy{Enabled: true, Min: 0, Max: 9, Default: 0}
	createTopic(ctx, t, b, topic)

	low := store.NewRecordAppend([]byte("low"))
	low.Headers = []store.RecordHeader{{Key: broker.PriorityHeader, Value: []byte("1")}}
	_, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{}, low)
	requireNoError(t, err)

	high := store.NewRecordAppend([]byte("high"))
	high.Headers = []store.RecordHeader{{Key: broker.PriorityHeader, Value: []byte("9")}}
	_, err = b.PublishRecord(ctx, "orders", broker.PublishPartitioning{}, high)
	requireNoError(t, err)

	poll, err := b.Fetch(ctx, "c1", "orders", 0, nil, 2)
	requireNoError(t, err)
	requireRecordPayloads(t, poll.Records, "high", "low")

	requireNoError(t, b.AckRecord(ctx, "c1", "orders", 0, poll.Records[0].Offset))
	state, err := b.CommittedOffset(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	requireOffsetState(t, state, 0, poll.Records[0].Offset)

	requireNoError(t, b.AckRecord(ctx, "c1", "orders", 0, poll.Records[1].Offset))
	state, err = b.CommittedOffset(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	requireOffsetState(t, state, 2)
}

func requireRecordPayloads(t *testing.T, records []store.Record, payloads ...string) {
	t.Helper()
	if len(records) != len(payloads) {
		t.Fatalf("records = %d, want %d", len(records), len(payloads))
	}
	for index, payload := range payloads {
		if string(records[index].Payload) != payload {
			t.Fatalf("record %d payload = %q, want %q", index, records[index].Payload, payload)
		}
	}
}

func requireOffsetState(t *testing.T, state *store.ConsumerOffsetState, nextOffset uint64, pending ...uint64) {
	t.Helper()
	if state == nil {
		t.Fatal("offset state is nil")
	}
	if state.NextOffset != nextOffset {
		t.Fatalf("next offset = %d, want %d", state.NextOffset, nextOffset)
	}
	requireUint64s(t, state.PendingOffsets, pending...)
}

func requireUint64s(t *testing.T, got []uint64, want ...uint64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("values = %v, want %v", got, want)
		}
	}
}
