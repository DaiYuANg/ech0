package broker_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"

	broker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func TestBrokerSingleNodeRaftProduceFetch(t *testing.T) {
	addr := freeTCPAddr(t)
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
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

func TestBrokerSingleNodeRaftClusterRouterResolvesShardPlacement(t *testing.T) {
	addr := freeTCPAddr(t)
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Broker.DataShardCount = 3
	cfg.Raft.BindAddr = addr
	cfg.Raft.HeartbeatIntervalMS = 50
	cfg.Raft.ElectionTimeoutMaxMS = 100
	cfg.Raft.ApplyTimeoutMS = 3000
	cfg.Raft.Cluster = []broker.RaftPeerConfig{{NodeID: cfg.Broker.NodeID, Addr: addr}}

	meta := store.NewMemoryStore()
	logStore := store.NewMemoryStore()
	b, err := broker.NewWithStores(cfg, logStore, meta)
	requireNoError(t, err)
	ctx := context.Background()
	requireNoError(t, b.Start(ctx))
	defer stopBroker(t, b)
	waitForLeader(t, b)

	topic := store.NewTopicConfig("orders")
	topic.Partitions = 4
	createTopic(ctx, t, b, topic)
	placement, err := meta.LoadShardPlacement(store.NewTopicPartition("orders", 2))
	requireNoError(t, err)
	if placement == nil || placement.ShardID != 2 {
		t.Fatalf("unexpected shard placement: %#v", placement)
	}

	result, err := b.PublishBatch(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 2}, []store.RecordAppend{
		{Payload: []byte("m1")},
	})
	requireNoError(t, err)
	if result.Partition != 2 || len(result.Records) != 1 {
		t.Fatalf("unexpected publish result: %#v", result)
	}
	poll, err := b.Fetch(ctx, "c1", "orders", 2, nil, 10)
	requireNoError(t, err)
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "m1" {
		t.Fatalf("unexpected poll result: %#v", poll)
	}
	requireNoError(t, b.CommitOffset(ctx, "c1", "orders", 2, poll.NextOffset))
}

func TestBrokerSingleNodeRaftCoalescedMultiShardProduceCommit(t *testing.T) {
	addr := freeTCPAddr(t)
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Broker.DataShardCount = 3
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

	topic := store.NewTopicConfig("orders")
	topic.Partitions = 3
	createTopic(ctx, t, b, topic)

	publishPartitionsConcurrently(ctx, t, b, []uint32{0, 1, 2})
	nextOffsets := assertPartitionsHaveOneRecord(ctx, t, b, []uint32{0, 1, 2})
	commitPartitionsConcurrently(ctx, t, b, nextOffsets)
	assertPartitionsCommitted(ctx, t, b, nextOffsets)
}

func TestBrokerRaftBindsUnspecifiedAndAdvertisesClusterAddress(t *testing.T) {
	addr := freeTCPAddr(t)
	_, port, err := net.SplitHostPort(addr)
	requireNoError(t, err)

	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
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

func publishPartitionsConcurrently(ctx context.Context, t *testing.T, b *broker.Broker, partitions []uint32) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, len(partitions))
	for _, partition := range partitions {
		wg.Go(func() {
			payload := strconv.FormatUint(uint64(partition), 10)
			result, err := b.PublishBatch(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: partition}, []store.RecordAppend{
				{Payload: []byte(payload)},
			})
			if err != nil {
				errs <- err
				return
			}
			if result.Partition != partition || len(result.Records) != 1 {
				errs <- fmt.Errorf("unexpected publish result for partition %d: %#v", partition, result)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		requireNoError(t, err)
	}
}

func assertPartitionsHaveOneRecord(ctx context.Context, t *testing.T, b *broker.Broker, partitions []uint32) *collectionmapping.Map[uint32, uint64] {
	t.Helper()
	nextOffsets := collectionmapping.NewMapWithCapacity[uint32, uint64](len(partitions))
	for _, partition := range partitions {
		poll, err := b.Fetch(ctx, "c1", "orders", partition, nil, 10)
		requireNoError(t, err)
		if len(poll.Records) != 1 || string(poll.Records[0].Payload) != strconv.FormatUint(uint64(partition), 10) {
			t.Fatalf("unexpected poll result for partition %d: %#v", partition, poll)
		}
		nextOffsets.Set(partition, poll.NextOffset)
	}
	return nextOffsets
}

func commitPartitionsConcurrently(ctx context.Context, t *testing.T, b *broker.Broker, nextOffsets *collectionmapping.Map[uint32, uint64]) {
	t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, nextOffsets.Len())
	nextOffsets.Range(func(partition uint32, nextOffset uint64) bool {
		wg.Go(func() {
			errs <- b.CommitOffset(ctx, "c1", "orders", partition, nextOffset)
		})
		return true
	})
	wg.Wait()
	close(errs)
	for err := range errs {
		requireNoError(t, err)
	}
}

func assertPartitionsCommitted(ctx context.Context, t *testing.T, b *broker.Broker, nextOffsets *collectionmapping.Map[uint32, uint64]) {
	t.Helper()
	nextOffsets.Range(func(partition uint32, nextOffset uint64) bool {
		poll, err := b.Fetch(ctx, "c1", "orders", partition, nil, 10)
		requireNoError(t, err)
		if len(poll.Records) != 0 || poll.NextOffset != nextOffset {
			t.Fatalf("unexpected committed poll result for partition %d: %#v", partition, poll)
		}
		return true
	})
}
