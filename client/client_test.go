package client_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lyonbrown4d/ech0/broker"
	ech0client "github.com/lyonbrown4d/ech0/client"
	"github.com/lyonbrown4d/ech0/protocol"
)

func TestTCPClientProduceFetchCommit(t *testing.T) {
	ctx := context.Background()
	remote := startRemoteBroker(ctx, t)
	defer remote.stop(t)

	_, err := remote.client.Admin().CreateTopic(ctx, "orders", ech0client.TopicPartitions(1))
	requireNoError(t, err)
	producer := remote.client.Producer("orders", ech0client.ProducerPartition(0))
	_, err = producer.Produce(ctx, []byte("m1"), ech0client.Key([]byte("k1")))
	requireNoError(t, err)

	consumer := remote.client.Consumer("c1", "orders", ech0client.ConsumerPartition(0), ech0client.ConsumerFetchLimit(10))
	fetched, err := consumer.Fetch(ctx, ech0client.FetchReadCommitted())
	requireNoError(t, err)
	if len(fetched.Records) != 1 || string(fetched.Records[0].Payload) != "m1" {
		t.Fatalf("unexpected fetch response: %#v", fetched)
	}
	_, err = consumer.Ack(ctx, fetched.Records[0])
	requireNoError(t, err)

	empty, err := consumer.Fetch(ctx)
	requireNoError(t, err)
	if len(empty.Records) != 0 || empty.NextOffset != 1 {
		t.Fatalf("expected committed fetch to be empty, got %#v", empty)
	}
}

func TestTCPClientTransactionAndRequestReply(t *testing.T) {
	ctx := context.Background()
	remote := startRemoteBroker(ctx, t)
	defer remote.stop(t)

	_, err := remote.client.Admin().CreateTopic(ctx, "orders")
	requireNoError(t, err)
	tx, err := remote.client.BeginTransaction(ctx, "worker-1")
	requireNoError(t, err)
	_, err = tx.Publish(ctx, "orders", []byte("tx-m1"), ech0client.Partition(0))
	requireNoError(t, err)

	consumer := remote.client.Consumer("tx-reader", "orders", ech0client.ConsumerReadCommitted())
	hidden, err := consumer.Fetch(ctx, ech0client.FetchLimit(10))
	requireNoError(t, err)
	if len(hidden.Records) != 0 {
		t.Fatalf("expected open transaction to be hidden, got %#v", hidden)
	}
	committed, err := tx.Commit(ctx)
	requireNoError(t, err)
	if committed.Status != protocol.TransactionStatusCommitted {
		t.Fatalf("unexpected transaction status: %#v", committed)
	}

	visible, err := consumer.Fetch(ctx, ech0client.FetchLimit(10))
	requireNoError(t, err)
	if len(visible.Records) != 1 || string(visible.Records[0].Payload) != "tx-m1" {
		t.Fatalf("expected committed transaction record, got %#v", visible)
	}

	_, err = remote.client.Admin().CreateTopic(ctx, "svc.echo")
	requireNoError(t, err)
	requester := remote.client.RequestReply("A1")
	responder := remote.client.RequestReply("B2")
	started, err := requester.Start(ctx, "svc.echo", []byte("ping"), ech0client.RequestPartition(0), ech0client.RequestTimeout(time.Second))
	requireNoError(t, err)
	requests, err := responder.FetchRequests(ctx, "svc-workers", "svc.echo", 0, 1)
	requireNoError(t, err)
	if len(requests.Requests) != 1 {
		t.Fatalf("expected one request, got %#v", requests)
	}
	_, err = responder.Reply(ctx, requests.Requests[0], []byte("pong"))
	requireNoError(t, err)
	reply, err := requester.AwaitReply(ctx, started)
	requireNoError(t, err)
	if reply.Reply.ResponderID != "B2" || string(reply.Reply.Payload) != "pong" {
		t.Fatalf("unexpected reply: %#v", reply)
	}
}

type remoteBroker struct {
	service *broker.Broker
	server  *broker.TCPServer
	client  *ech0client.Client
}

func startRemoteBroker(ctx context.Context, t *testing.T) remoteBroker {
	t.Helper()
	cfg := broker.DefaultConfig()
	cfg.Broker.BindAddr = freeAddr(ctx, t)
	cfg.Broker.RetryWorkerEnabled = false
	cfg.Broker.DelaySchedulerEnabled = false
	service, err := broker.New(cfg)
	requireNoError(t, err)
	server := broker.NewTCPServer(cfg, service, nil, nil)
	requireNoError(t, server.Start(ctx))
	client, err := ech0client.Dial(ctx, server.Addr().String(), ech0client.WithOperationTimeout(time.Second))
	requireNoError(t, err)
	return remoteBroker{service: service, server: server, client: client}
}

func (r remoteBroker) stop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	requireNoError(t, r.client.Close(ctx))
	requireNoError(t, r.server.Stop(ctx))
	requireNoError(t, r.service.Stop(ctx))
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

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
