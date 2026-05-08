package broker_test

import (
	"context"
	"net"
	"testing"

	broker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
)

func TestBrokerSingleNodeRaftProduceFetch(t *testing.T) {
	addr := freeTCPAddr(t)
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Raft.Enabled = true
	cfg.Raft.BindAddr = addr
	cfg.Raft.HeartbeatIntervalMS = 50
	cfg.Raft.ElectionTimeoutMaxMS = 100
	cfg.Raft.ApplyTimeoutMS = 3000
	cfg.Raft.Cluster = []broker.RaftPeerConfig{{NodeID: cfg.Broker.NodeID, Addr: addr}}

	b, err := broker.New(cfg)
	requireNoError(t, err)
	ctx := context.Background()
	requireNoError(t, b.Start(ctx))
	defer stopBroker(t, b)
	waitForLeader(t, b)

	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	requirePollM1(t, poll)
}

func TestBrokerSingleNodeRaftBatchProduceCommit(t *testing.T) {
	addr := freeTCPAddr(t)
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Raft.Enabled = true
	cfg.Raft.BindAddr = addr
	cfg.Raft.HeartbeatIntervalMS = 50
	cfg.Raft.ElectionTimeoutMaxMS = 100
	cfg.Raft.ApplyTimeoutMS = 3000
	cfg.Raft.Cluster = []broker.RaftPeerConfig{{NodeID: cfg.Broker.NodeID, Addr: addr}}

	b, err := broker.New(cfg)
	requireNoError(t, err)
	ctx := context.Background()
	requireNoError(t, b.Start(ctx))
	defer stopBroker(t, b)
	waitForLeader(t, b)

	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	result, err := b.PublishBatch(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, []store.RecordAppend{
		{Key: []byte("k1"), Payload: []byte("m1")},
		{Key: []byte("k2"), Payload: []byte("m2")},
	})
	requireNoError(t, err)
	if result.Partition != 0 || len(result.Records) != 2 {
		t.Fatalf("unexpected batch produce result: %#v", result)
	}
	poll, err := b.Fetch(ctx, "c1", "orders", 0, nil, 10)
	requireNoError(t, err)
	if len(poll.Records) != 2 || string(poll.Records[0].Payload) != "m1" || string(poll.Records[1].Payload) != "m2" {
		t.Fatalf("unexpected poll result: %#v", poll)
	}
	requireNoError(t, b.CommitOffset(ctx, "c1", "orders", 0, poll.NextOffset))
	poll, err = b.Fetch(ctx, "c1", "orders", 0, nil, 10)
	requireNoError(t, err)
	if len(poll.Records) != 0 || poll.NextOffset != 2 {
		t.Fatalf("unexpected committed poll result: %#v", poll)
	}
}

func TestBrokerRaftBindsUnspecifiedAndAdvertisesClusterAddress(t *testing.T) {
	addr := freeTCPAddr(t)
	_, port, err := net.SplitHostPort(addr)
	requireNoError(t, err)

	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Raft.Enabled = true
	cfg.Raft.BindAddr = net.JoinHostPort("0.0.0.0", port)
	cfg.Raft.HeartbeatIntervalMS = 50
	cfg.Raft.ElectionTimeoutMaxMS = 100
	cfg.Raft.ApplyTimeoutMS = 3000
	cfg.Raft.Cluster = []broker.RaftPeerConfig{
		{NodeID: cfg.Broker.NodeID, Addr: net.JoinHostPort("127.0.0.1", port)},
	}

	b, err := broker.New(cfg)
	requireNoError(t, err)
	ctx := context.Background()
	requireNoError(t, b.Start(ctx))
	defer stopBroker(t, b)
	waitForLeader(t, b)
}
