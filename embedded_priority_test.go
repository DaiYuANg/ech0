package ech0_test

import (
	"context"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedPriorityPolicyAndPublishOption(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders", ech0.PriorityRange(1, 5, 2)))
	msg, err := b.Publish(ctx, "orders", []byte("high"), ech0.Priority(5))
	requireNoError(t, err)
	if got := headerValue(msg.Headers, "x-ech0-priority"); got != "5" {
		t.Fatalf("priority header = %q, want 5", got)
	}

	if _, err := b.Publish(ctx, "orders", []byte("too-high"), ech0.Priority(9)); err == nil {
		t.Fatal("expected out-of-range priority to be rejected")
	}
}

func TestEmbeddedPriorityFetchAckDoesNotSkipLowerOffsets(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders", ech0.PriorityRange(0, 9, 0)))
	_, err := b.Publish(ctx, "orders", []byte("low"), ech0.Priority(1))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "orders", []byte("high"), ech0.Priority(9))
	requireNoError(t, err)

	batch, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(2))
	requireNoError(t, err)
	if len(batch.Messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(batch.Messages))
	}
	if string(batch.Messages[0].Payload) != "high" || string(batch.Messages[1].Payload) != "low" {
		t.Fatalf("unexpected priority order: %q then %q", batch.Messages[0].Payload, batch.Messages[1].Payload)
	}

	requireNoError(t, b.Ack(ctx, "c1", batch.Messages[0]))
	replayed, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(2))
	requireNoError(t, err)
	if len(replayed.Messages) != 1 || string(replayed.Messages[0].Payload) != "low" {
		t.Fatalf("unexpected messages after high ack: %#v", replayed.Messages)
	}

	requireNoError(t, b.Ack(ctx, "c1", batch.Messages[1]))
	drained, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(2))
	requireNoError(t, err)
	if len(drained.Messages) != 0 {
		t.Fatalf("messages after contiguous ack = %d, want 0", len(drained.Messages))
	}
}

func headerValue(headers []ech0.Header, key string) string {
	for index := range headers {
		if headers[index].Key == key {
			return string(headers[index].Value)
		}
	}
	return ""
}
