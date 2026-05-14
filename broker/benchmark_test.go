package broker_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/lyonbrown4d/ech0/transport"
)

var brokerBenchmarkProduceSink broker.ProduceResult
var brokerBenchmarkPollSink store.PollResult
var brokerBenchmarkFrameSink transport.Frame

func BenchmarkBrokerPublishMemory1KB(b *testing.B) {
	br := newBenchmarkBroker(b)
	ctx := context.Background()
	createBenchmarkTopic(ctx, b, br, "orders")
	payload := benchmarkBrokerPayload(1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		result, err := br.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, payload)
		if err != nil {
			b.Fatal(err)
		}
		brokerBenchmarkProduceSink = result
	}
}

func BenchmarkBrokerPublishFetchCommitMemory1KB(b *testing.B) {
	br := newBenchmarkBroker(b)
	ctx := context.Background()
	createBenchmarkTopic(ctx, b, br, "orders")
	payload := benchmarkBrokerPayload(1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; b.Loop(); i++ {
		if _, err := br.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, payload); err != nil {
			b.Fatal(err)
		}
		poll, err := br.Fetch(ctx, "consumer", "orders", 0, nil, 1)
		if err != nil {
			b.Fatal(err)
		}
		if len(poll.Records) != 1 {
			b.Fatalf("expected one record at iteration %d, got %d", i, len(poll.Records))
		}
		if err := br.CommitOffset(ctx, "consumer", "orders", 0, poll.NextOffset); err != nil {
			b.Fatal(err)
		}
		brokerBenchmarkPollSink = poll
	}
}

func BenchmarkBrokerTCPProduceFrameMemory1KB(b *testing.B) {
	br := newBenchmarkBroker(b)
	ctx := context.Background()
	server := broker.NewTCPServer(broker.DefaultConfig(), br, nil, nil)
	createBenchmarkTopic(ctx, b, br, "orders")
	partition := uint32(0)
	req := protocol.ProduceRequest{
		Topic:        "orders",
		Partition:    &partition,
		Partitioning: protocol.ProducePartitioningExplicit,
		Key:          []byte("customer-42"),
		Headers:      []protocol.MessageHeader{{Key: "trace_id", Value: []byte("trace-1")}},
		Payload:      benchmarkBrokerPayload(1024),
	}
	body, err := protocol.EncodeBody(protocol.CmdProduceRequest, req)
	mustBenchmarkNoError(b, err)
	frame, err := transport.NewFrame(protocol.Version, protocol.CmdProduceRequest, body)
	mustBenchmarkNoError(b, err)
	b.ReportAllocs()
	b.SetBytes(int64(len(req.Payload)))
	for b.Loop() {
		response, err := server.HandleFrame(ctx, frame)
		if err != nil {
			b.Fatal(err)
		}
		brokerBenchmarkFrameSink = response
	}
}

func BenchmarkBrokerRequestReplyMemory512B(b *testing.B) {
	br := newBenchmarkBroker(b)
	ctx := context.Background()
	createBenchmarkTopic(ctx, b, br, "svc.echo")
	payload := benchmarkBrokerPayload(512)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for i := 0; b.Loop(); i++ {
		benchmarkRequestReplyRoundTrip(ctx, b, br, payload, i)
	}
}

func BenchmarkBrokerPublishParallelMemory1KB(b *testing.B) {
	br := newBenchmarkBroker(b)
	ctx := context.Background()
	createBenchmarkTopic(ctx, b, br, "orders")
	payload := benchmarkBrokerPayload(1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.RunParallel(func(pb *testing.PB) {
		sequence := 0
		for pb.Next() {
			result, err := br.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionKeyHash}, []byte(strconv.Itoa(sequence)), false, payload)
			if err != nil {
				b.Fatal(err)
			}
			brokerBenchmarkProduceSink = result
			sequence++
		}
	})
}

func newBenchmarkBroker(b *testing.B) *broker.Broker {
	b.Helper()
	br, err := broker.New(broker.DefaultConfig())
	mustBenchmarkNoError(b, err)
	return br
}

func createBenchmarkTopic(ctx context.Context, b *testing.B, br *broker.Broker, name string) {
	b.Helper()
	_, err := br.CreateTopic(ctx, store.NewTopicConfig(name))
	mustBenchmarkNoError(b, err)
}

func benchmarkRequestReplyRoundTrip(ctx context.Context, b *testing.B, br *broker.Broker, payload []byte, iteration int) {
	b.Helper()
	pending, err := br.StartRequest(ctx, "svc.echo", payload, broker.RequestOptions{
		InstanceID:   "A1",
		Timeout:      time.Second,
		PollInterval: time.Microsecond,
		Partitioning: broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0},
	})
	mustBenchmarkNoError(b, err)
	requests, err := br.FetchRequests(ctx, "workers", "svc.echo", 0, nil, 1)
	mustBenchmarkNoError(b, err)
	if len(requests.Requests) != 1 {
		b.Fatalf("expected one request at iteration %d, got %d", iteration, len(requests.Requests))
	}
	_, err = br.Reply(ctx, requests.Requests[0], "B1", payload)
	mustBenchmarkNoError(b, err)
	_, err = br.AwaitReply(ctx, pending)
	mustBenchmarkNoError(b, err)
	mustBenchmarkNoError(b, br.CommitOffset(ctx, "workers", "svc.echo", 0, requests.NextOffset))
}

func benchmarkBrokerPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}

func mustBenchmarkNoError(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
