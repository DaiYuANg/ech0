package ech0_test

import (
	"context"
	"testing"
	"time"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedDirectInbox(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	sent, err := b.SendDirect(ctx, "alice", "bob", []byte("hello"), ech0.ConversationID("chat-1"))
	requireNoError(t, err)
	if sent.MessageID == "" || sent.ConversationID != "chat-1" {
		t.Fatalf("unexpected direct send result: %#v", sent)
	}
	inbox, err := b.FetchInbox(ctx, "bob", ech0.InboxLimit(10))
	requireNoError(t, err)
	if len(inbox.Messages) != 1 || inbox.Messages[0].Sender != "alice" || string(inbox.Messages[0].Payload) != "hello" {
		t.Fatalf("unexpected inbox: %#v", inbox)
	}
	requireNoError(t, b.AckDirect(ctx, "bob", inbox.NextOffset))
	inbox, err = b.FetchInbox(ctx, "bob", ech0.InboxLimit(10))
	requireNoError(t, err)
	if len(inbox.Messages) != 0 {
		t.Fatalf("expected empty inbox after ack, got %#v", inbox)
	}
}

func TestEmbeddedRequestReply(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "svc.echo"))
	pending, err := b.StartRequest(ctx, "svc.echo", []byte("ping"),
		ech0.RequestInstance("A1"),
		ech0.RequestTimeout(time.Second),
		ech0.RequestPollInterval(time.Millisecond),
		ech0.RequestPartition(0),
		ech0.RequestHeader("trace_id", []byte("trace-1")),
	)
	requireNoError(t, err)
	if pending.ReplyTo != "__reply/A1" {
		t.Fatalf("unexpected reply inbox: %#v", pending)
	}
	requests, err := b.FetchRequests(ctx, "workers", "svc.echo", ech0.FetchLimit(1))
	requireNoError(t, err)
	if len(requests.Requests) != 1 || string(requests.Requests[0].Payload) != "ping" {
		t.Fatalf("unexpected requests: %#v", requests)
	}
	if publicHeaderValue(requests.Requests[0].Headers, "trace_id") != "trace-1" {
		t.Fatalf("request header missing: %#v", requests.Requests[0].Headers)
	}
	_, err = b.Reply(ctx, requests.Requests[0], "B1", []byte("pong"))
	requireNoError(t, err)
	reply, err := b.AwaitReply(ctx, pending)
	requireNoError(t, err)
	if reply.ResponderID != "B1" || reply.CorrelationID != pending.CorrelationID || string(reply.Payload) != "pong" {
		t.Fatalf("unexpected reply: %#v", reply)
	}
}

func TestEmbeddedRequestManyCollectsReplies(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "svc.echo"))
	pending, err := b.StartRequest(ctx, "svc.echo", []byte("ping"),
		ech0.RequestInstance("A1"),
		ech0.RequestTimeout(time.Second),
		ech0.RequestPollInterval(time.Millisecond),
		ech0.RequestPartition(0),
		ech0.RequestMode(ech0.RequestReplyModeMultiReplier),
	)
	requireNoError(t, err)
	first, err := b.FetchRequests(ctx, "workers-b1", "svc.echo", ech0.FetchLimit(1))
	requireNoError(t, err)
	second, err := b.FetchRequests(ctx, "workers-b2", "svc.echo", ech0.FetchLimit(1))
	requireNoError(t, err)
	if len(first.Requests) != 1 || len(second.Requests) != 1 {
		t.Fatalf("unexpected requests: first=%#v second=%#v", first, second)
	}
	_, err = b.Reply(ctx, first.Requests[0], "B1", []byte("one"))
	requireNoError(t, err)
	_, err = b.Reply(ctx, second.Requests[0], "B2", []byte("two"))
	requireNoError(t, err)

	replies, err := b.AwaitReplies(ctx, pending, 2)
	requireNoError(t, err)
	if len(replies) != 2 || replies[0].ResponderID != "B1" || replies[1].ResponderID != "B2" {
		t.Fatalf("unexpected replies: %#v", replies)
	}
}

func publicHeaderValue(headers []ech0.Header, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
