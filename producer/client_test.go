package producer_test

import (
	"context"
	"net"
	"testing"
	"time"

	ech0 "github.com/lyonbrown4d/ech0"
	"github.com/lyonbrown4d/ech0/producer"
)

func TestProducerClientSendsMessages(t *testing.T) {
	ctx := context.Background()
	b := openBroker(ctx, t)
	defer closeBroker(ctx, t, b)
	requireNoError(t, b.CreateTopic(ctx, "orders"))

	client, err := producer.New(ctx, b, "orders", producer.BatchSize(2), producer.Linger(time.Millisecond))
	requireNoError(t, err)
	defer func() {
		requireNoError(t, client.Close(ctx))
	}()
	future, err := client.Send(ctx, []byte("m1"), ech0.Key([]byte("k1")))
	requireNoError(t, err)
	msg, err := future.Await(ctx)
	requireNoError(t, err)
	if msg.Topic != "orders" || string(msg.Payload) != "m1" {
		t.Fatalf("unexpected produced message: %#v", msg)
	}

	fetched, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(1))
	requireNoError(t, err)
	if len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected fetched messages: %#v", fetched)
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
