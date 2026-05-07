package broker_test

import (
	"context"
	"strings"
	"testing"

	broker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/DaiYuANg/ech0/transport"
)

func TestTCPProduceFetchHeaders(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	server := broker.NewTCPServer(broker.DefaultConfig(), b, nil, nil)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	partition := uint32(0)

	handleProtocolFrame[protocol.ProduceResponse](ctx, t, server, protocol.CmdProduceRequest, protocol.CmdProduceResponse, protocol.ProduceRequest{
		Topic:        "orders",
		Partition:    &partition,
		Partitioning: protocol.ProducePartitioningExplicit,
		Headers:      []protocol.MessageHeader{{Key: "trace_id", Value: []byte("trace-1")}},
		Payload:      []byte("m1"),
	})
	fetched := handleProtocolFrame[protocol.FetchResponse](ctx, t, server, protocol.CmdFetchRequest, protocol.CmdFetchResponse, protocol.FetchRequest{
		Consumer:   "c1",
		Topic:      "orders",
		Partition:  0,
		MaxRecords: 1,
	})
	if len(fetched.Records) != 1 || protocolHeaderValue(fetched.Records[0].Headers, "trace_id") != "trace-1" {
		t.Fatalf("unexpected fetch response: %#v", fetched)
	}
}

func TestTCPRequestReplyProtocolRoutesToOriginInstance(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	server := broker.NewTCPServer(broker.DefaultConfig(), b, nil, nil)
	createTopic(ctx, t, b, store.NewTopicConfig("svc.echo"))

	started := startRequestViaProtocol(ctx, t, server)
	requests := fetchRequestsViaProtocol(ctx, t, server)
	if len(requests.Requests) != 1 {
		t.Fatalf("expected one request, got %#v", requests)
	}
	req := requests.Requests[0]
	if req.ReplyTo != started.ReplyTo || req.SenderID != "A1" || protocolHeaderValue(req.Headers, "trace_id") != "trace-1" {
		t.Fatalf("unexpected request record: %#v", req)
	}

	replyViaProtocol(ctx, t, server, req)
	awaited := awaitReplyViaProtocol(ctx, t, server, started)
	if awaited.Reply.ResponderID != "B2" || string(awaited.Reply.Payload) != "pong" {
		t.Fatalf("unexpected reply: %#v", awaited)
	}

	a2ReplyInbox := strings.Replace(started.ReplyTo, "/A1/", "/A2/", 1)
	inbox, err := b.FetchInbox(a2ReplyInbox, 10)
	requireNoError(t, err)
	if len(inbox.Records) != 0 {
		t.Fatalf("expected A2 reply inbox to stay empty, got %#v", inbox)
	}
}

func startRequestViaProtocol(ctx context.Context, t *testing.T, server *broker.TCPServer) protocol.StartRequestResponse {
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
			Headers:        []protocol.MessageHeader{{Key: "trace_id", Value: []byte("trace-1")}},
			Payload:        []byte("ping"),
		},
	)
}

func fetchRequestsViaProtocol(ctx context.Context, t *testing.T, server *broker.TCPServer) protocol.FetchRequestsResponse {
	t.Helper()
	return handleProtocolFrame[protocol.FetchRequestsResponse](
		ctx,
		t,
		server,
		protocol.CmdFetchRequestsRequest,
		protocol.CmdFetchRequestsResponse,
		protocol.FetchRequestsRequest{Consumer: "svc-workers", Subject: "svc.echo", Partition: 0, MaxRecords: 1},
	)
}

func replyViaProtocol(ctx context.Context, t *testing.T, server *broker.TCPServer, req protocol.RequestRecord) {
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
			ResponderID:   "B2",
			Payload:       []byte("pong"),
		},
	)
}

func awaitReplyViaProtocol(ctx context.Context, t *testing.T, server *broker.TCPServer, started protocol.StartRequestResponse) protocol.AwaitReplyResponse {
	t.Helper()
	pollIntervalMS := uint64(1)
	return handleProtocolFrame[protocol.AwaitReplyResponse](
		ctx,
		t,
		server,
		protocol.CmdAwaitReplyRequest,
		protocol.CmdAwaitReplyResponse,
		protocol.AwaitReplyRequest{
			ReplyTo:        started.ReplyTo,
			CorrelationID:  started.CorrelationID,
			ExpiresAtMS:    started.ExpiresAtMS,
			PollIntervalMS: &pollIntervalMS,
		},
	)
}

func handleProtocolFrame[T any](
	ctx context.Context,
	t *testing.T,
	server *broker.TCPServer,
	requestCommand uint16,
	responseCommand uint16,
	req any,
) T {
	t.Helper()
	body, err := protocol.EncodeBody(requestCommand, req)
	requireNoError(t, err)
	frame, err := transport.NewFrame(protocol.Version, requestCommand, body)
	requireNoError(t, err)
	response, err := server.HandleFrame(ctx, frame)
	requireNoError(t, err)
	if response.Header.Command == protocol.CmdErrorResponse {
		var out protocol.ErrorResponse
		requireNoError(t, protocol.DecodeBody(protocol.CmdErrorResponse, response.Body, &out))
		t.Fatalf("protocol error %s: %s", out.Code, out.Message)
	}
	if response.Header.Command != responseCommand {
		t.Fatalf("unexpected response command %d, want %d", response.Header.Command, responseCommand)
	}
	var out T
	err = protocol.DecodeBody(responseCommand, response.Body, &out)
	requireNoError(t, err)
	return out
}

func protocolHeaderValue(headers []protocol.MessageHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}
