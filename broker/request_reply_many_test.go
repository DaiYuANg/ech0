package broker_test

import (
	"context"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerAwaitReplyUsesFirstResponseWins(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("svc.echo"))

	pending := startModeRequest(ctx, t, b, broker.RequestReplyModeFirstResponseWins)
	first := fetchOneRequest(ctx, t, b, "svc-b1")
	second := fetchOneRequest(ctx, t, b, "svc-b2")
	if first.ReplyMode != broker.RequestReplyModeFirstResponseWins || second.CorrelationID != pending.CorrelationID {
		t.Fatalf("unexpected request records: first=%#v second=%#v", first, second)
	}
	replyAs(ctx, t, b, first, "B1", "one")
	replyAs(ctx, t, b, second, "B2", "two")

	reply, err := b.AwaitReply(ctx, pending)
	requireNoError(t, err)
	if reply.SenderID != "B1" || string(reply.Payload) != "one" {
		t.Fatalf("expected first response to win, got %#v", reply)
	}
}

func TestBrokerAwaitRepliesCollectsMultiReplierResponses(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("svc.echo"))

	pending := startModeRequest(ctx, t, b, broker.RequestReplyModeMultiReplier)
	first := fetchOneRequest(ctx, t, b, "svc-b1")
	second := fetchOneRequest(ctx, t, b, "svc-b2")
	if first.ReplyMode != broker.RequestReplyModeMultiReplier || second.ReplyMode != broker.RequestReplyModeMultiReplier {
		t.Fatalf("expected multi replier request records, got %#v and %#v", first, second)
	}
	replyAs(ctx, t, b, first, "B1", "one")
	replyAs(ctx, t, b, second, "B2", "two")

	replies, err := b.AwaitReplies(ctx, pending, 2)
	requireNoError(t, err)
	if len(replies) != 2 {
		t.Fatalf("expected two replies, got %#v", replies)
	}
	if replies[0].SenderID != "B1" || replies[1].SenderID != "B2" {
		t.Fatalf("unexpected reply order: %#v", replies)
	}
}

func startModeRequest(ctx context.Context, t *testing.T, b *broker.Broker, mode broker.RequestReplyMode) broker.PendingRequest {
	t.Helper()
	pending, err := b.StartRequest(ctx, "svc.echo", []byte("ping"), broker.RequestOptions{
		InstanceID:   "A1",
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
		Partitioning: broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0},
		ReplyMode:    mode,
	})
	requireNoError(t, err)
	return pending
}

func fetchOneRequest(ctx context.Context, t *testing.T, b *broker.Broker, consumer string) broker.RequestMessage {
	t.Helper()
	requests, err := b.FetchRequests(ctx, consumer, "svc.echo", 0, nil, 1)
	requireNoError(t, err)
	if len(requests.Requests) != 1 {
		t.Fatalf("expected one request for %s, got %#v", consumer, requests)
	}
	return requests.Requests[0]
}

func replyAs(ctx context.Context, t *testing.T, b *broker.Broker, request broker.RequestMessage, responderID, payload string) {
	t.Helper()
	_, err := b.Reply(ctx, request, responderID, []byte(payload))
	requireNoError(t, err)
}
