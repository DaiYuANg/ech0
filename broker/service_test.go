//nolint:testpackage // Same-package tests exercise unexported broker internals.
package broker

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/DaiYuANg/ech0/store"
)

//nolint:cyclop,gocyclo,gocognit // This test keeps the produce/fetch/commit flow linear.
func TestBrokerProduceFetchCommit(t *testing.T) {
	b, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, createErr := b.CreateTopic(ctx, store.NewTopicConfig("orders")); createErr != nil {
		t.Fatal(createErr)
	}
	produced, err := b.Publish(ctx, "orders", PublishPartitioning{Mode: PartitionExplicit, Partition: 0}, nil, false, []byte("m1"))
	if err != nil {
		t.Fatal(err)
	}
	if produced.Partition != 0 || produced.Record.Offset != 0 {
		t.Fatalf("unexpected produce result: %#v", produced)
	}
	poll, err := b.Fetch("c1", "orders", 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "m1" || poll.NextOffset != 1 {
		t.Fatalf("unexpected poll result: %#v", poll)
	}
	if commitErr := b.CommitOffset(ctx, "c1", "orders", 0, poll.NextOffset); commitErr != nil {
		t.Fatal(commitErr)
	}
	poll, err = b.Fetch("c1", "orders", 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(poll.Records) != 0 || poll.NextOffset != 1 {
		t.Fatalf("unexpected committed poll result: %#v", poll)
	}
}

//nolint:cyclop,gocyclo // This test keeps the direct inbox flow linear.
func TestBrokerDirectInbox(t *testing.T) {
	b, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	sent, err := b.SendDirect(ctx, "alice", "bob", nil, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if sent.Offset != 0 || sent.NextOffset != 1 || sent.MessageID == "" {
		t.Fatalf("unexpected direct result: %#v", sent)
	}
	inbox, err := b.FetchInbox("bob", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox.Records) != 1 || string(inbox.Records[0].Message.Payload) != "hello" {
		t.Fatalf("unexpected inbox result: %#v", inbox)
	}
	if ackErr := b.AckDirect(ctx, "bob", inbox.NextOffset); ackErr != nil {
		t.Fatal(ackErr)
	}
	inbox, err = b.FetchInbox("bob", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(inbox.Records) != 0 {
		t.Fatalf("expected empty inbox after ack, got %#v", inbox)
	}
}

//nolint:cyclop,gocyclo,gocognit // This test keeps retry behavior assertions in one flow.
func TestBrokerNackAndProcessRetry(t *testing.T) {
	b, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.RetryPolicy = store.TopicRetryPolicy{MaxAttempts: 3, BackoffInitialMS: 1, BackoffMaxMS: 1}
	if _, createErr := b.CreateTopic(ctx, topic); createErr != nil {
		t.Fatal(createErr)
	}
	if _, publishErr := b.Publish(ctx, "orders", PublishPartitioning{Mode: PartitionExplicit, Partition: 0}, nil, false, []byte("m1")); publishErr != nil {
		t.Fatal(publishErr)
	}
	lastErr := "db timeout"
	retried, err := b.Nack(ctx, "c1", "orders", 0, 0, &lastErr)
	if err != nil {
		t.Fatal(err)
	}
	if retried.RetryTopic != "__retry.orders" || retried.RetryPartition != 0 || retried.RetryCount != 1 {
		t.Fatalf("unexpected retry result: %#v", retried)
	}
	time.Sleep(2 * time.Millisecond)
	processed, err := b.ProcessRetryBatch(ctx, "retry-worker", "orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed.MovedToOrigin != 1 || processed.MovedToDeadLetter != 0 || processed.CommittedNextOffset == nil || *processed.CommittedNextOffset != 1 {
		t.Fatalf("unexpected process retry result: %#v", processed)
	}
	records, err := b.log.ReadFrom(store.NewTopicPartition("orders", 0), 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || string(records[0].Payload) != "m1" {
		t.Fatalf("expected retried record at origin offset 1, got %#v", records)
	}
}

//nolint:cyclop,gocyclo // This test keeps DLQ behavior assertions in one flow.
func TestBrokerRetryExhaustionMovesToDefaultDLQ(t *testing.T) {
	b, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.RetryPolicy = store.TopicRetryPolicy{MaxAttempts: 1, BackoffInitialMS: 1, BackoffMaxMS: 1}
	if _, createErr := b.CreateTopic(ctx, topic); createErr != nil {
		t.Fatal(createErr)
	}
	if _, publishErr := b.Publish(ctx, "orders", PublishPartitioning{Mode: PartitionExplicit, Partition: 0}, nil, false, []byte("m1")); publishErr != nil {
		t.Fatal(publishErr)
	}
	lastErr := "failed"
	if _, nackErr := b.Nack(ctx, "c1", "orders", 0, 0, &lastErr); nackErr != nil {
		t.Fatal(nackErr)
	}
	time.Sleep(2 * time.Millisecond)
	processed, err := b.ProcessRetryBatch(ctx, "retry-worker", "orders", 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if processed.MovedToOrigin != 0 || processed.MovedToDeadLetter != 1 {
		t.Fatalf("unexpected process retry result: %#v", processed)
	}
	records, err := b.log.ReadFrom(store.NewTopicPartition("__dlq.orders", 0), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || headerString(records[0].Headers, dlqHeaderErrorCode) != dlqErrorCodeRetryExhausted {
		t.Fatalf("unexpected dlq records: %#v", records)
	}
}

//nolint:cyclop,gocyclo,gocognit // This test keeps delayed delivery assertions in one flow.
func TestBrokerScheduleDelayAndProcessDue(t *testing.T) {
	b, err := New(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, createErr := b.CreateTopic(ctx, store.NewTopicConfig("orders")); createErr != nil {
		t.Fatal(createErr)
	}
	deliverAt := store.NowMS() + 1
	scheduled, err := b.ScheduleDelay(ctx, "orders", 0, []byte("m1"), deliverAt)
	if err != nil {
		t.Fatal(err)
	}
	if scheduled.DelayTopic != "__delay.orders" || scheduled.DeliverAtMS != deliverAt {
		t.Fatalf("unexpected delay result: %#v", scheduled)
	}
	moved, err := b.ProcessDueDelayedOnce(ctx, "__delay_scheduler", 10)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 0 {
		t.Fatalf("expected delayed record to stay before deliver_at, moved %d", moved)
	}
	time.Sleep(2 * time.Millisecond)
	moved, err = b.ProcessDueDelayedOnce(ctx, "__delay_scheduler", 10)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 1 {
		t.Fatalf("expected one delayed record to move, moved %d", moved)
	}
	records, err := b.log.ReadFrom(store.NewTopicPartition("orders", 0), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || string(records[0].Payload) != "m1" {
		t.Fatalf("unexpected delayed records: %#v", records)
	}
}

func TestBrokerSingleNodeRaftProduceFetch(t *testing.T) {
	addr := freeTCPAddr(t)
	cfg := DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Raft.Enabled = true
	cfg.Raft.BindAddr = addr
	cfg.Raft.HeartbeatIntervalMS = 50
	cfg.Raft.ElectionTimeoutMaxMS = 100
	cfg.Raft.ApplyTimeoutMS = 3000
	cfg.Raft.Cluster = []RaftPeerConfig{{NodeID: cfg.Broker.NodeID, Addr: addr}}

	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if startErr := b.Start(ctx); startErr != nil {
		t.Fatal(startErr)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if stopErr := b.Stop(stopCtx); stopErr != nil {
			t.Fatal(stopErr)
		}
	}()
	waitForLeader(t, b)

	if _, createErr := b.CreateTopic(ctx, store.NewTopicConfig("orders")); createErr != nil {
		t.Fatal(createErr)
	}
	if _, publishErr := b.Publish(ctx, "orders", PublishPartitioning{Mode: PartitionExplicit, Partition: 0}, nil, false, []byte("m1")); publishErr != nil {
		t.Fatal(publishErr)
	}
	poll, err := b.Fetch("c1", "orders", 0, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "m1" {
		t.Fatalf("unexpected raft poll result: %#v", poll)
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := listener.Close(); err != nil {
			t.Logf("close listener: %v", err)
		}
	}()
	return listener.Addr().String()
}

func waitForLeader(t *testing.T, b *Broker) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		health := b.RuntimeHealth()
		if health.Raft != nil && health.Raft.LocalIsLeader {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("raft leader not elected: %#v", b.RuntimeHealth())
}
