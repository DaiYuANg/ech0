package ech0_test

import (
	"context"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedBrokerMinimalFlow(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	produced := publishEmbedded(ctx, t, b, "orders", []byte("m1"))
	if produced.Topic != "orders" || produced.NextOffset != produced.Offset+1 {
		t.Fatalf("unexpected produced message: %#v", produced)
	}
	fetched := fetchEmbedded(ctx, t, b, "orders")
	if len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected fetched messages: %#v", fetched)
	}
	requireNoError(t, b.Ack(ctx, "c1", fetched.Messages[0]))
	fetched = fetchEmbedded(ctx, t, b, "orders")
	if len(fetched.Messages) != 0 {
		t.Fatalf("expected no messages after ack, got %#v", fetched)
	}
}

func TestEmbeddedTransactionFlow(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	tx, err := b.BeginTransaction(ctx, "worker-1")
	requireNoError(t, err)
	_, err = tx.Publish(ctx, "orders", []byte("m1"))
	requireNoError(t, err)

	hidden, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(10), ech0.ReadCommitted())
	requireNoError(t, err)
	if len(hidden.Messages) != 0 {
		t.Fatalf("expected read_committed to hide open transaction, got %#v", hidden)
	}
	requireNoError(t, tx.Commit(ctx))
	visible, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(10), ech0.ReadCommitted())
	requireNoError(t, err)
	if len(visible.Messages) != 1 || string(visible.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected committed transaction fetch: %#v", visible)
	}
}

func openEmbeddedBroker(ctx context.Context, t *testing.T) *ech0.Broker {
	t.Helper()
	b, err := ech0.Open(ctx, ech0.Options{DataDir: t.TempDir()})
	requireNoError(t, err)
	return b
}

func closeEmbeddedBroker(ctx context.Context, t *testing.T, b *ech0.Broker) {
	t.Helper()
	requireNoError(t, b.Close(ctx))
}

func publishEmbedded(ctx context.Context, t *testing.T, b *ech0.Broker, topic string, payload []byte) ech0.Message {
	t.Helper()
	produced, err := b.Publish(ctx, topic, payload)
	requireNoError(t, err)
	return produced
}

func fetchEmbedded(ctx context.Context, t *testing.T, b *ech0.Broker, topic string) ech0.FetchResult {
	t.Helper()
	fetched, err := b.Fetch(ctx, "c1", topic, ech0.FetchLimit(10))
	requireNoError(t, err)
	return fetched
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
