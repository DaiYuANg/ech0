package broker_test

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/protocol"
	protocolbinary "github.com/lyonbrown4d/ech0/protocol/binary"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/lyonbrown4d/ech0/transport"
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

func TestTCPHandshakeNegotiatesCapabilities(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	server := broker.NewTCPServer(broker.DefaultConfig(), b, nil, nil)
	resp := handleProtocolFrame[protocol.HandshakeResponse](ctx, t, server, protocol.CmdHandshakeRequest, protocol.CmdHandshakeResponse, protocol.HandshakeRequest{
		ClientID: "client-1",
		Capabilities: []string{
			protocol.CapabilityTransactions,
			"not-supported",
			protocol.CapabilityCompressionZstd,
		},
	})
	want := []string{protocol.CapabilityCompressionZstd, protocol.CapabilityTransactions}
	if !stringSlicesEqual(resp.Capabilities, want) {
		t.Fatalf("unexpected capabilities: got %#v want %#v", resp.Capabilities, want)
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

	a2ReplyInbox := strings.Replace(started.ReplyTo, "/A1", "/A2", 1)
	inbox, err := b.FetchInbox(a2ReplyInbox, 10)
	requireNoError(t, err)
	if len(inbox.Records) != 0 {
		t.Fatalf("expected A2 reply inbox to stay empty, got %#v", inbox)
	}
}

func TestTCPTransactionProtocolCommitsReadCommittedRecords(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	server := broker.NewTCPServer(broker.DefaultConfig(), b, nil, nil)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	partition := uint32(0)

	begin := handleProtocolFrame[protocol.TxBeginResponse](ctx, t, server, protocol.CmdTxBeginRequest, protocol.CmdTxBeginResponse, protocol.TxBeginRequest{
		TransactionalID: "worker-1",
		TimeoutMS:       30_000,
	})
	if begin.Identity.TxID == 0 || begin.Status != protocol.TransactionStatusOpen {
		t.Fatalf("unexpected begin response: %#v", begin)
	}
	handleProtocolFrame[protocol.TxPublishResponse](ctx, t, server, protocol.CmdTxPublishRequest, protocol.CmdTxPublishResponse, protocol.TxPublishRequest{
		Identity:     begin.Identity,
		Sequence:     0,
		Topic:        "orders",
		Partition:    &partition,
		Partitioning: protocol.ProducePartitioningExplicit,
		Payload:      []byte("m1"),
	})

	beforeCommit := handleProtocolFrame[protocol.FetchResponse](ctx, t, server, protocol.CmdFetchRequest, protocol.CmdFetchResponse, protocol.FetchRequest{
		Consumer:   "c1",
		Topic:      "orders",
		Partition:  0,
		MaxRecords: 1,
		Isolation:  protocol.FetchIsolationReadCommitted,
	})
	if len(beforeCommit.Records) != 0 || beforeCommit.NextOffset != 0 {
		t.Fatalf("expected transactional record to be hidden before commit, got %#v", beforeCommit)
	}

	committed := handleProtocolFrame[protocol.TxCommitResponse](ctx, t, server, protocol.CmdTxCommitRequest, protocol.CmdTxCommitResponse, protocol.TxCommitRequest{
		Identity: begin.Identity,
	})
	if committed.Status != protocol.TransactionStatusCommitted {
		t.Fatalf("unexpected commit response: %#v", committed)
	}
	afterCommit := handleProtocolFrame[protocol.FetchResponse](ctx, t, server, protocol.CmdFetchRequest, protocol.CmdFetchResponse, protocol.FetchRequest{
		Consumer:   "c1",
		Topic:      "orders",
		Partition:  0,
		MaxRecords: 1,
		Isolation:  protocol.FetchIsolationReadCommitted,
	})
	if len(afterCommit.Records) != 1 || string(afterCommit.Records[0].Payload) != "m1" {
		t.Fatalf("expected committed transactional record, got %#v", afterCommit)
	}
}

func TestTCPServerAcceptsConnectionsAfterStartContextCanceled(t *testing.T) {
	b := newTestBroker(t)
	cfg := broker.DefaultConfig()
	cfg.Broker.BindAddr = "127.0.0.1:0"
	server := broker.NewTCPServer(cfg, b, nil, nil)
	startCtx, cancelStart := context.WithCancel(context.Background())
	requireNoError(t, server.Start(startCtx))
	cancelStart()
	t.Cleanup(func() {
		stopCtx, cancelStop := context.WithTimeout(context.Background(), time.Second)
		defer cancelStop()
		requireNoError(t, server.Stop(stopCtx))
	})

	addr := server.Addr()
	if addr == nil {
		t.Fatal("tcp server address is nil")
	}
	dialCtx, cancelDial := context.WithTimeout(context.Background(), time.Second)
	defer cancelDial()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "tcp", addr.String())
	requireNoError(t, err)
	defer func() {
		requireNoError(t, conn.Close())
	}()

	body, err := protocolbinary.EncodeBody(protocol.CmdPingRequest, protocol.PingRequest{Nonce: 42})
	requireNoError(t, err)
	frame, err := transport.NewFrame(protocol.Version, protocol.CmdPingRequest, body)
	requireNoError(t, err)
	requireNoError(t, transport.WriteFrame(conn, frame))
	response, err := transport.ReadFrame(conn)
	requireNoError(t, err)
	if response.Header.Command != protocol.CmdPingResponse {
		t.Fatalf("unexpected response command %d", response.Header.Command)
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

func handleProtocolFrame[T any](ctx context.Context, t *testing.T, server *broker.TCPServer, requestCommand, responseCommand uint16, req any) T {
	t.Helper()
	body, err := protocolbinary.EncodeBody(requestCommand, req)
	requireNoError(t, err)
	frame, err := transport.NewFrame(protocol.Version, requestCommand, body)
	requireNoError(t, err)
	response, err := server.HandleFrame(ctx, frame)
	requireNoError(t, err)
	if response.Header.Command == protocol.CmdErrorResponse {
		var out protocol.ErrorResponse
		requireNoError(t, protocolbinary.DecodeBody(protocol.CmdErrorResponse, response.Body, &out))
		t.Fatalf("protocol error %s: %s", out.Code, out.Message)
	}
	if response.Header.Command != responseCommand {
		t.Fatalf("unexpected response command %d, want %d", response.Header.Command, responseCommand)
	}
	var out T
	err = protocolbinary.DecodeBody(responseCommand, response.Body, &out)
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

func stringSlicesEqual(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
