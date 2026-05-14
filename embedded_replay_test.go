package ech0_test

import (
	"context"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedReplayFromOffsetDoesNotAdvanceConsumer(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	publishEmbedded(ctx, t, b, []byte("m1"))
	publishEmbedded(ctx, t, b, []byte("m2"))

	replayed, err := b.ReplayFromOffset(ctx, "orders", 1, ech0.ReplayLimit(10))
	requireNoError(t, err)
	requireEmbeddedReplayPayloads(t, replayed, "m2")

	fetched := fetchEmbedded(ctx, t, b)
	requireEmbeddedFetchPayloads(t, fetched, "m1", "m2")
}

func TestEmbeddedReplayFromCursorPagesRecords(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	publishEmbedded(ctx, t, b, []byte("m1"))
	publishEmbedded(ctx, t, b, []byte("m2"))
	publishEmbedded(ctx, t, b, []byte("m3"))

	first, err := b.ReplayFromCursor(ctx, "orders", "", ech0.ReplayLimit(2))
	requireNoError(t, err)
	if !first.HasMore || first.NextCursor == "" {
		t.Fatalf("unexpected first replay page: %#v", first)
	}
	requireEmbeddedReplayPayloads(t, first, "m1", "m2")

	second, err := b.ReplayFromCursor(ctx, "orders", first.NextCursor, ech0.ReplayLimit(2))
	requireNoError(t, err)
	if second.HasMore {
		t.Fatalf("unexpected second replay page: %#v", second)
	}
	requireEmbeddedReplayPayloads(t, second, "m3")
}

func requireEmbeddedReplayPayloads(t *testing.T, result ech0.ReplayResult, payloads ...string) {
	t.Helper()
	if len(result.Messages) != len(payloads) {
		t.Fatalf("unexpected replay result: %#v", result)
	}
	for index, payload := range payloads {
		if string(result.Messages[index].Payload) != payload {
			t.Fatalf("unexpected replay payload at %d: %#v", index, result)
		}
	}
}

func requireEmbeddedFetchPayloads(t *testing.T, result ech0.FetchResult, payloads ...string) {
	t.Helper()
	if len(result.Messages) != len(payloads) {
		t.Fatalf("unexpected fetch result: %#v", result)
	}
	for index, payload := range payloads {
		if string(result.Messages[index].Payload) != payload {
			t.Fatalf("unexpected fetch payload at %d: %#v", index, result)
		}
	}
}
