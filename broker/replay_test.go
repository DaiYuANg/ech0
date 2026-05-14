package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerReplayFromOffsetDoesNotAdvanceConsumer(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	publishOrder(ctx, t, b, []byte("m2"))
	publishOrder(ctx, t, b, []byte("m3"))

	replayed, err := b.ReplayFromOffset(ctx, "orders", 0, 1, 10)
	requireNoError(t, err)
	requireReplayPayloads(t, replayed, "m2", "m3")

	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePayloads(t, poll, "m1", "m2", "m3")
}

func TestBrokerReplayFromTimestampStartsAtFirstMatchingRecord(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishTimestamped(ctx, t, b, "m1", 10)
	publishTimestamped(ctx, t, b, "m2", 20)
	publishTimestamped(ctx, t, b, "m3", 30)

	replayed, err := b.ReplayFromTimestamp(ctx, "orders", 0, 20, 10)
	requireNoError(t, err)
	if replayed.Offset != 1 {
		t.Fatalf("unexpected replay offset: %#v", replayed)
	}
	requireReplayPayloads(t, replayed, "m2", "m3")
}

func TestBrokerReplayFromCursorPagesRecords(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	publishOrder(ctx, t, b, []byte("m2"))
	publishOrder(ctx, t, b, []byte("m3"))

	first, err := b.ReplayFromCursor(ctx, "orders", 0, "", 2)
	requireNoError(t, err)
	if !first.HasMore || first.NextCursor == "" {
		t.Fatalf("unexpected first replay page: %#v", first)
	}
	requireReplayPayloads(t, first, "m1", "m2")

	second, err := b.ReplayFromCursor(ctx, "orders", 0, first.NextCursor, 2)
	requireNoError(t, err)
	if second.HasMore {
		t.Fatalf("unexpected second replay page: %#v", second)
	}
	requireReplayPayloads(t, second, "m3")
}

func requireReplayPayloads(t *testing.T, result broker.ReplayResult, payloads ...string) {
	t.Helper()
	if len(result.Records) != len(payloads) {
		t.Fatalf("unexpected replay result: %#v", result)
	}
	for index, payload := range payloads {
		if string(result.Records[index].Payload) != payload {
			t.Fatalf("unexpected replay payload at %d: %#v", index, result)
		}
	}
}
