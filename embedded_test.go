package ech0_test

import (
	"context"
	"net"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedBrokerMinimalFlow(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	produced := publishEmbedded(ctx, t, b, []byte("m1"))
	if produced.Topic != "orders" || produced.NextOffset != produced.Offset+1 {
		t.Fatalf("unexpected produced message: %#v", produced)
	}
	fetched := fetchEmbedded(ctx, t, b)
	if len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected fetched messages: %#v", fetched)
	}
	requireNoError(t, b.Ack(ctx, "c1", fetched.Messages[0]))
	fetched = fetchEmbedded(ctx, t, b)
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

func TestEmbeddedSeekOffset(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	publishEmbedded(ctx, t, b, []byte("m1"))
	publishEmbedded(ctx, t, b, []byte("m2"))

	result, err := b.SeekOffset(ctx, "c1", "orders", 0, 1)
	requireNoError(t, err)
	if result.Offset != 1 || result.Topic != "orders" {
		t.Fatalf("unexpected seek result: %#v", result)
	}
	fetched := fetchEmbedded(ctx, t, b)
	if len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "m2" {
		t.Fatalf("unexpected fetched messages after seek: %#v", fetched)
	}
}

func openEmbeddedBroker(ctx context.Context, t *testing.T) *ech0.Broker {
	t.Helper()
	return openEmbeddedBrokerWithOptions(ctx, t, ech0.Options{})
}

func openEmbeddedBrokerWithOptions(ctx context.Context, t *testing.T, opts ech0.Options) *ech0.Broker {
	t.Helper()
	opts.DataDir = t.TempDir()
	opts.Raft = &ech0.RaftOptions{BindAddr: freeTCPAddr(ctx, t)}
	b, err := ech0.Open(ctx, opts)
	requireNoError(t, err)
	return b
}

func freeTCPAddr(ctx context.Context, t *testing.T) string {
	t.Helper()
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "tcp", "127.0.0.1:0")
	requireNoError(t, err)
	defer func() {
		requireNoError(t, listener.Close())
	}()
	return listener.Addr().String()
}

func closeEmbeddedBroker(ctx context.Context, t *testing.T, b *ech0.Broker) {
	t.Helper()
	requireNoError(t, b.Close(ctx))
}

func publishEmbedded(ctx context.Context, t *testing.T, b *ech0.Broker, payload []byte) ech0.Message {
	t.Helper()
	produced, err := b.Publish(ctx, "orders", payload)
	requireNoError(t, err)
	return produced
}

func fetchEmbedded(ctx context.Context, t *testing.T, b *ech0.Broker) ech0.FetchResult {
	t.Helper()
	fetched, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(10))
	requireNoError(t, err)
	return fetched
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
