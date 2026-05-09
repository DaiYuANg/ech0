package broker_test

import (
	"context"
	"strings"
	"testing"
	"time"

	broker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
)

func TestBrokerProduceFetchCommit(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	produced := publishOrder(ctx, t, b, []byte("m1"))
	if produced.Partition != 0 || produced.Record.Offset != 0 {
		t.Fatalf("unexpected produce result: %#v", produced)
	}
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePollM1(t, poll)

	requireNoError(t, b.CommitOffset(ctx, "c1", "orders", 0, poll.NextOffset))
	poll = fetchTopic(t, b, "c1", "orders", nil, 10)
	if len(poll.Records) != 0 || poll.NextOffset != 1 {
		t.Fatalf("unexpected committed poll result: %#v", poll)
	}
}

func TestBrokerDirectInbox(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()

	sent, err := b.SendDirect(ctx, "alice", "bob", nil, []byte("hello"))
	requireNoError(t, err)
	if sent.Offset != 0 || sent.NextOffset != 1 || sent.MessageID == "" {
		t.Fatalf("unexpected direct result: %#v", sent)
	}
	inbox, err := b.FetchInbox("bob", 10)
	requireNoError(t, err)
	if len(inbox.Records) != 1 || string(inbox.Records[0].Message.Payload) != "hello" {
		t.Fatalf("unexpected inbox result: %#v", inbox)
	}

	requireNoError(t, b.AckDirect(ctx, "bob", inbox.NextOffset))
	inbox, err = b.FetchInbox("bob", 10)
	requireNoError(t, err)
	if len(inbox.Records) != 0 {
		t.Fatalf("expected empty inbox after ack, got %#v", inbox)
	}
}

func TestBrokerRequestReplyRoutesToOriginInstance(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("svc.echo"))

	pending, err := b.StartRequest(ctx, "svc.echo", []byte("ping"), broker.RequestOptions{
		InstanceID:   " A1 ",
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
		Partitioning: broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0},
	})
	requireNoError(t, err)
	if !strings.HasPrefix(pending.ReplyTo, "__reply/A1/") {
		t.Fatalf("expected reply inbox to be scoped to A1, got %q", pending.ReplyTo)
	}

	requests, err := b.FetchRequests(ctx, "svc-workers", "svc.echo", 0, nil, 1)
	requireNoError(t, err)
	if len(requests.Requests) != 1 {
		t.Fatalf("expected one request, got %#v", requests)
	}
	request := requests.Requests[0]
	if request.SenderID != "A1" || request.ReplyTo != pending.ReplyTo || string(request.Payload) != "ping" {
		t.Fatalf("unexpected request message: %#v", request)
	}

	_, err = b.Reply(ctx, request, "B2", []byte("pong"))
	requireNoError(t, err)
	reply, err := b.AwaitReply(ctx, pending)
	requireNoError(t, err)
	if reply.SenderID != "B2" || reply.CorrelationID != pending.CorrelationID || string(reply.Payload) != "pong" {
		t.Fatalf("unexpected reply message: %#v", reply)
	}

	a2ReplyInbox := strings.Replace(pending.ReplyTo, "/A1/", "/A2/", 1)
	inbox, err := b.FetchInbox(a2ReplyInbox, 10)
	requireNoError(t, err)
	if len(inbox.Records) != 0 {
		t.Fatalf("expected A2 reply inbox to stay empty, got %#v", inbox)
	}
}

func TestBrokerNackAndProcessRetry(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, retryTopic("orders", 3))
	publishOrder(ctx, t, b, []byte("m1"))

	lastErr := "db timeout"
	retried, err := b.Nack(ctx, "c1", "orders", 0, 0, &lastErr)
	requireNoError(t, err)
	if retried.RetryTopic != "__retry.orders" || retried.RetryPartition != 0 || retried.RetryCount != 1 {
		t.Fatalf("unexpected retry result: %#v", retried)
	}

	time.Sleep(2 * time.Millisecond)
	processed, err := b.ProcessRetryBatch(ctx, "retry-worker", "orders", 0, 10)
	requireNoError(t, err)
	if processed.MovedToOrigin != 1 || processed.MovedToDeadLetter != 0 || processed.CommittedNextOffset == nil || *processed.CommittedNextOffset != 1 {
		t.Fatalf("unexpected process retry result: %#v", processed)
	}
	offset := uint64(1)
	poll := fetchTopic(t, b, "c2", "orders", &offset, 1)
	requirePollM1(t, poll)
}

func TestBrokerRetryExhaustionMovesToDefaultDLQ(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, retryTopic("orders", 1))
	publishOrder(ctx, t, b, []byte("m1"))

	lastErr := "failed"
	_, err := b.Nack(ctx, "c1", "orders", 0, 0, &lastErr)
	requireNoError(t, err)
	time.Sleep(2 * time.Millisecond)
	processed, err := b.ProcessRetryBatch(ctx, "retry-worker", "orders", 0, 10)
	requireNoError(t, err)
	if processed.MovedToOrigin != 0 || processed.MovedToDeadLetter != 1 {
		t.Fatalf("unexpected process retry result: %#v", processed)
	}
	poll := fetchTopic(t, b, "dlq-reader", "__dlq.orders", nil, 1)
	if len(poll.Records) != 1 || headerValue(poll.Records[0].Headers, "x-dlq-error-code") != "retry_exhausted" {
		t.Fatalf("unexpected dlq records: %#v", poll.Records)
	}
}

func TestBrokerScheduleDelayAndProcessDue(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	deliverAt := store.NowMS() + 50
	scheduled, err := b.ScheduleDelay(ctx, "orders", 0, []byte("m1"), deliverAt)
	requireNoError(t, err)
	if scheduled.DelayTopic != "__delay.orders" || scheduled.DeliverAtMS != deliverAt {
		t.Fatalf("unexpected delay result: %#v", scheduled)
	}

	moved := processDueDelayed(ctx, t, b)
	if moved != 0 {
		t.Fatalf("expected delayed record to stay before deliver_at, moved %d", moved)
	}
	time.Sleep(60 * time.Millisecond)
	moved = processDueDelayed(ctx, t, b)
	if moved != 1 {
		t.Fatalf("expected one delayed record to move, moved %d", moved)
	}
	poll := fetchTopic(t, b, "delay-reader", "orders", nil, 1)
	requirePollM1(t, poll)
}

func newTestBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b, err := broker.New(broker.DefaultConfig())
	requireNoError(t, err)
	return b
}

func createTopic(ctx context.Context, t *testing.T, b *broker.Broker, topic store.TopicConfig) {
	t.Helper()
	_, err := b.CreateTopic(ctx, topic)
	requireNoError(t, err)
}

func publishOrder(ctx context.Context, t *testing.T, b *broker.Broker, payload []byte) broker.ProduceResult {
	t.Helper()
	result, err := b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, payload)
	requireNoError(t, err)
	return result
}

func fetchTopic(t *testing.T, b *broker.Broker, consumer, topic string, offset *uint64, maxRecords int) store.PollResult {
	t.Helper()
	poll, err := b.Fetch(context.Background(), consumer, topic, 0, offset, maxRecords)
	requireNoError(t, err)
	return poll
}

func processDueDelayed(ctx context.Context, t *testing.T, b *broker.Broker) int {
	t.Helper()
	moved, err := b.ProcessDueDelayedOnce(ctx, "__delay_scheduler", 10)
	requireNoError(t, err)
	return moved
}

func retryTopic(name string, maxAttempts uint32) store.TopicConfig {
	topic := store.NewTopicConfig(name)
	topic.RetryPolicy = store.TopicRetryPolicy{MaxAttempts: maxAttempts, BackoffInitialMS: 1, BackoffMaxMS: 1}
	return topic
}

func requirePollM1(t *testing.T, poll store.PollResult) {
	t.Helper()
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "m1" {
		t.Fatalf("unexpected poll result: %#v", poll)
	}
}

func headerValue(headers []store.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
