package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
)

func TestTCPAwaitRepliesProtocolCollectsMultiReplierResponses(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	server := broker.NewTCPServer(broker.DefaultConfig(), b, nil, nil)
	createTopic(ctx, t, b, store.NewTopicConfig("svc.echo"))

	started := startMultiRequestViaProtocol(ctx, t, server)
	first := fetchRequestsForConsumer(ctx, t, server, "svc-b1").Requests[0]
	second := fetchRequestsForConsumer(ctx, t, server, "svc-b2").Requests[0]
	replyViaProtocolAs(ctx, t, server, first, "B1", "one")
	replyViaProtocolAs(ctx, t, server, second, "B2", "two")

	pollIntervalMS := uint64(1)
	replies := handleProtocolFrame[protocol.AwaitRepliesResponse](
		ctx,
		t,
		server,
		protocol.CmdAwaitRepliesRequest,
		protocol.CmdAwaitRepliesResponse,
		protocol.AwaitRepliesRequest{
			ReplyTo:        started.ReplyTo,
			CorrelationID:  started.CorrelationID,
			ExpiresAtMS:    started.ExpiresAtMS,
			PollIntervalMS: &pollIntervalMS,
			MaxReplies:     2,
		},
	)
	if len(replies.Replies) != 2 || replies.Replies[0].ResponderID != "B1" || replies.Replies[1].ResponderID != "B2" {
		t.Fatalf("unexpected replies: %#v", replies)
	}
}

func startMultiRequestViaProtocol(ctx context.Context, t *testing.T, server *broker.TCPServer) protocol.StartRequestResponse {
	t.Helper()
	timeoutMS := uint64(1000)
	pollIntervalMS := uint64(1)
	partition := uint32(0)
	return handleProtocolFrame[protocol.StartRequestResponse](
		ctx,
		t,
		server,
		protocol.CmdStartRequestRequest,
		protocol.CmdStartRequestResponse,
		protocol.StartRequestRequest{
			Subject:        "svc.echo",
			InstanceID:     "A1",
			TimeoutMS:      &timeoutMS,
			PollIntervalMS: &pollIntervalMS,
			Partition:      &partition,
			Partitioning:   protocol.ProducePartitioningExplicit,
			ReplyMode:      protocol.RequestReplyModeMultiReplier,
			Payload:        []byte("ping"),
		},
	)
}

func fetchRequestsForConsumer(ctx context.Context, t *testing.T, server *broker.TCPServer, consumer string) protocol.FetchRequestsResponse {
	t.Helper()
	return handleProtocolFrame[protocol.FetchRequestsResponse](
		ctx,
		t,
		server,
		protocol.CmdFetchRequestsRequest,
		protocol.CmdFetchRequestsResponse,
		protocol.FetchRequestsRequest{Consumer: consumer, Subject: "svc.echo", Partition: 0, MaxRecords: 1},
	)
}

func replyViaProtocolAs(ctx context.Context, t *testing.T, server *broker.TCPServer, req protocol.RequestRecord, responderID, payload string) {
	t.Helper()
	handleProtocolFrame[protocol.ReplyResponse](
		ctx,
		t,
		server,
		protocol.CmdReplyRequest,
		protocol.CmdReplyResponse,
		protocol.ReplyRequest{
			Subject:       req.Subject,
			ReplyTo:       req.ReplyTo,
			CorrelationID: req.CorrelationID,
			ExpiresAtMS:   req.ExpiresAtMS,
			ResponderID:   responderID,
			Payload:       []byte(payload),
		},
	)
}
