package broker_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerSingleNodeRaftTransactionRecoversAfterRestart(t *testing.T) {
	ctx := context.Background()
	cfg := singleNodeRecoveryConfig(t)
	b := startRecoveryBroker(t, cfg)

	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	tx, err := b.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = b.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)
	_, err = b.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	stopBroker(t, b)

	restarted := startRecoveryBroker(t, cfg)
	defer stopBroker(t, restarted)

	poll, err := restarted.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePollM1(t, poll)
	next, err := restarted.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	if next.Identity.ProducerID != tx.Identity.ProducerID || next.Identity.ProducerEpoch != tx.Identity.ProducerEpoch+1 {
		t.Fatalf("expected recovered transaction lineage to advance epoch, previous=%#v next=%#v", tx.Identity, next.Identity)
	}
}

func TestBrokerRaftTransactionCommitsAfterLeaderFailover(t *testing.T) {
	ctx := context.Background()
	brokers := startRecoveryCluster(t, 3)
	leader := waitForAnyLeader(t, brokers)

	createTopic(ctx, t, leader, store.NewTopicConfig("orders"))
	tx, err := leader.BeginTransaction(ctx, "worker-1", 30_000)
	requireNoError(t, err)
	_, err = leader.PublishTransactionalRecord(ctx, tx.Identity, 0, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, store.NewRecordAppend([]byte("m1")))
	requireNoError(t, err)
	stopClusterBroker(t, brokers, leader)

	newLeader := waitForAnyLeader(t, brokers)
	_, err = newLeader.CommitTransaction(ctx, tx.Identity)
	requireNoError(t, err)
	poll, err := newLeader.FetchWithIsolation(ctx, "c1", "orders", 0, nil, 10, broker.FetchIsolationReadCommitted)
	requireNoError(t, err)
	requirePollM1(t, poll)
}

func singleNodeRecoveryConfig(t *testing.T) broker.Config {
	t.Helper()
	addr := freeTCPAddr(t)
	cfg := recoveryBaseConfig(t, 1, addr)
	cfg.Raft.Cluster = []broker.RaftPeerConfig{{NodeID: 1, Addr: addr}}
	return cfg
}

func startRecoveryCluster(t *testing.T, count int) []*broker.Broker {
	t.Helper()
	peers := make([]broker.RaftPeerConfig, 0, count)
	for node := 1; node <= count; node++ {
		peers = append(peers, broker.RaftPeerConfig{NodeID: uint64(node), Addr: freeTCPAddr(t)})
	}
	brokers := make([]*broker.Broker, 0, count)
	for _, peer := range peers {
		cfg := recoveryBaseConfig(t, peer.NodeID, peer.Addr)
		cfg.Raft.Cluster = peers
		brokers = append(brokers, startRecoveryBroker(t, cfg))
	}
	t.Cleanup(func() {
		for _, b := range brokers {
			if b != nil {
				stopBroker(t, b)
			}
		}
	})
	return brokers
}

func recoveryBaseConfig(t *testing.T, nodeID uint64, raftAddr string) broker.Config {
	t.Helper()
	cfg := broker.DefaultConfig()
	cfg.Broker.NodeID = nodeID
	cfg.Broker.ClusterName = "ech0-transaction-recovery"
	cfg.Broker.DataDir = filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", nodeID))
	cfg.Raft.BindAddr = raftAddr
	cfg.Raft.AdvertiseAddr = raftAddr
	cfg.Raft.HeartbeatIntervalMS = 50
	cfg.Raft.ElectionTimeoutMinMS = 100
	cfg.Raft.ElectionTimeoutMaxMS = 200
	cfg.Raft.ApplyTimeoutMS = 5000
	return cfg
}

func startRecoveryBroker(t *testing.T, cfg broker.Config) *broker.Broker {
	t.Helper()
	b, err := broker.New(cfg)
	requireNoError(t, err)
	requireNoError(t, b.Start(context.Background()))
	return b
}

func stopClusterBroker(t *testing.T, brokers []*broker.Broker, target *broker.Broker) {
	t.Helper()
	for index, b := range brokers {
		if b == target {
			stopBroker(t, b)
			brokers[index] = nil
			return
		}
	}
	t.Fatal("cluster broker not found")
}
