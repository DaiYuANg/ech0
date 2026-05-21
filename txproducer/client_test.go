package txproducer_test

import (
	"context"
	"net"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
	"github.com/lyonbrown4d/ech0/txproducer"
)

func TestTransactionalProducerClientCommitsMessages(t *testing.T) {
	ctx := context.Background()
	b := openBroker(ctx, t)
	defer closeBroker(ctx, t, b)
	requireNoError(t, b.CreateTopic(ctx, "orders"))

	client, err := txproducer.New(ctx, b, "tx-orders")
	requireNoError(t, err)
	_, err = client.Publish(ctx, "orders", []byte("m1"))
	requireNoError(t, err)

	beforeCommit, err := b.Fetch(ctx, "c1", "orders", ech0.ReadCommitted(), ech0.FetchLimit(1))
	requireNoError(t, err)
	if len(beforeCommit.Messages) != 0 {
		t.Fatalf("expected transaction to be hidden before commit, got %#v", beforeCommit)
	}
	requireNoError(t, client.Commit(ctx))

	afterCommit, err := b.Fetch(ctx, "c1", "orders", ech0.ReadCommitted(), ech0.FetchLimit(1))
	requireNoError(t, err)
	if len(afterCommit.Messages) != 1 || string(afterCommit.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected committed messages: %#v", afterCommit)
	}
}

func openBroker(ctx context.Context, t *testing.T) *ech0.Broker {
	t.Helper()
	opts := ech0.DefaultOptions()
	opts.DataDir = t.TempDir()
	opts.DisableDelay = true
	opts.DisableRetry = true
	addr := freeAddr(ctx, t)
	opts.Raft = &ech0.RaftOptions{
		BindAddr: addr,
		Peers:    []ech0.RaftPeer{{NodeID: opts.NodeID, Addr: addr}},
	}
	b, err := ech0.Open(ctx, opts)
	requireNoError(t, err)
	return b
}

func freeAddr(ctx context.Context, t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	requireNoError(t, err)
	defer func() {
		requireNoError(t, listener.Close())
	}()
	return listener.Addr().String()
}

func closeBroker(ctx context.Context, t *testing.T, b *ech0.Broker) {
	t.Helper()
	requireNoError(t, b.Close(ctx))
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
