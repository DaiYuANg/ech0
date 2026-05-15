package broker_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerRaftFollowerRestartFromExistingData(t *testing.T) {
	ctx := context.Background()
	brokers, configs := startRecoveryClusterWithConfigs(t, 3)
	leader := waitForAnyLeader(t, brokers)

	createTopicWithRetry(ctx, t, brokers, store.NewTopicConfig("orders"))
	for i := range 3 {
		publishOrderWithRetry(ctx, t, brokers, fmt.Appendf(nil, "msg-%d", i))
	}

	followerIndex, follower := nonDataShardLeaderBroker(t, brokers, leader)
	recordsBeforeStop := fetchAllRecordsOrFail(t, follower, "orders", 3)
	if len(recordsBeforeStop) != 3 {
		t.Fatalf("expected follower to catch up before restart, got %d records", len(recordsBeforeStop))
	}
	stopClusterBroker(t, brokers, follower)

	restarted := startRecoveryBroker(t, configs[followerIndex])
	t.Cleanup(func() {
		stopBroker(t, restarted)
	})
	waitForRaftNodeReady(t, restarted)

	records := fetchAllRecordsOrFail(t, restarted, "orders", 3)
	if len(records) != 3 {
		t.Fatalf("expected 3 records after follower restart recovery, got %d", len(records))
	}
}

func createTopicWithRetry(ctx context.Context, t *testing.T, brokers []*broker.Broker, topic store.TopicConfig) {
	t.Helper()
	retryBrokerOperation(t, "create topic from current raft leader", func() []*broker.Broker {
		return brokers
	}, func(candidate *broker.Broker) error {
		_, err := candidate.CreateTopic(ctx, topic)
		if err != nil {
			return fmt.Errorf("create recovery topic: %w", err)
		}

		return nil
	})
}

func publishOrderWithRetry(ctx context.Context, t *testing.T, brokers []*broker.Broker, payload []byte) {
	t.Helper()
	retryBrokerOperation(t, "publish order to current raft leader", func() []*broker.Broker {
		candidates := preferredDataShardLeaders(brokers)
		if len(candidates) > 0 {
			return candidates
		}
		return brokers
	}, func(candidate *broker.Broker) error {
		return capturePublishError(ctx, candidate, payload)
	})
}

func retryBrokerOperation(
	t *testing.T,
	description string,
	candidates func() []*broker.Broker,
	operation func(*broker.Broker) error,
) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if tryBrokerCandidates(t, candidates(), operation) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("%s timed out", description)
}

func tryBrokerCandidates(t *testing.T, candidates []*broker.Broker, operation func(*broker.Broker) error) bool {
	t.Helper()
	for _, candidate := range candidates {
		if tryBrokerCandidate(t, candidate, operation) {
			return true
		}
	}
	return false
}

func tryBrokerCandidate(t *testing.T, candidate *broker.Broker, operation func(*broker.Broker) error) bool {
	t.Helper()
	if candidate == nil {
		return false
	}
	err := operation(candidate)
	if err == nil {
		return true
	}
	if isRaftTransientError(err) {
		return false
	}
	requireNoError(t, err)
	return false
}

func capturePublishError(ctx context.Context, b *broker.Broker, payload []byte) error {
	_, err := b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, payload)
	if err != nil {
		return fmt.Errorf("publish recovery order: %w", err)
	}
	return nil
}

func fetchAllRecordsOrFail(t *testing.T, b *broker.Broker, topic string, expected int) []store.Record {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		poll, err := b.Fetch(context.Background(), "c1", topic, 0, nil, expected)
		if err == nil && len(poll.Records) >= expected {
			return poll.Records
		}
		time.Sleep(25 * time.Millisecond)
	}
	poll, err := b.Fetch(context.Background(), "c1", topic, 0, nil, expected)
	requireNoError(t, err)
	return poll.Records
}

func nonDataShardLeaderBroker(t *testing.T, brokers []*broker.Broker, metadataLeader *broker.Broker) (int, *broker.Broker) {
	t.Helper()
	fallbackIndex := -1
	var fallback *broker.Broker
	for index, candidate := range brokers {
		if candidate == nil || dataShardLocalLeader(candidate) {
			continue
		}
		if candidate != metadataLeader {
			return index, candidate
		}
		fallbackIndex = index
		fallback = candidate
	}
	if fallback != nil {
		return fallbackIndex, fallback
	}
	t.Fatal("expected a non-data-shard-leader broker in cluster")
	return -1, nil
}

func waitForRaftNodeReady(t *testing.T, b *broker.Broker) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if raftRuntimeReady(b) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("raft node did not become ready after restart: %#v", b.RuntimeHealth())
}

func raftRuntimeReady(b *broker.Broker) bool {
	if b == nil {
		return false
	}
	return raftGroupsReady(b.RuntimeHealth().Raft)
}

func raftGroupsReady(health *broker.RaftHealth) bool {
	if health == nil || len(health.Groups) == 0 {
		return false
	}
	for _, group := range health.Groups {
		if !group.Ready {
			return false
		}
	}
	return true
}

func isRaftTransientError(err error) bool {
	code := store.ErrorCode(err)
	return code == store.CodeNotLeader || code == store.CodeUnavailable
}

func startRecoveryClusterWithConfigs(t *testing.T, count int) ([]*broker.Broker, []broker.Config) {
	t.Helper()
	peers := make([]broker.RaftPeerConfig, 0, count)
	for node := 1; node <= count; node++ {
		peers = append(peers, broker.RaftPeerConfig{NodeID: uint64(node), Addr: freeTCPAddr(t)})
	}
	brokers := make([]*broker.Broker, 0, count)
	configs := make([]broker.Config, 0, count)
	tmpDir := t.TempDir()
	clusterName := fmt.Sprintf("ech0-cluster-restart-%d", time.Now().UnixNano())
	for _, peer := range peers {
		cfg := recoveryBaseConfig(t, peer.NodeID, peer.Addr)
		cfg.Broker.ClusterName = clusterName
		cfg.Raft.Cluster = peers
		cfg.Raft.ReadPolicy = broker.RaftReadLinearizable
		cfg.Broker.DataDir = filepath.Join(tmpDir, fmt.Sprintf("node-%d", peer.NodeID))
		cfg.Discovery.Enabled = false
		brokers = append(brokers, startRecoveryBroker(t, cfg))
		configs = append(configs, cfg)
	}
	t.Cleanup(func() {
		for _, broker := range brokers {
			if broker != nil {
				stopBroker(t, broker)
			}
		}
	})
	return brokers, configs
}

func preferredDataShardLeaders(brokers []*broker.Broker) []*broker.Broker {
	out := make([]*broker.Broker, 0, len(brokers))
	for _, b := range brokers {
		if dataShardLocalLeader(b) {
			out = append(out, b)
		}
	}
	return out
}

func dataShardLocalLeader(b *broker.Broker) bool {
	if b == nil || b.RuntimeHealth().Raft == nil {
		return false
	}
	for _, group := range b.RuntimeHealth().Raft.Groups {
		if group.GroupID != 1 && group.LocalIsLeader {
			return true
		}
	}
	return false
}
