package broker_test

import (
	"context"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
)

func assertClusterLeaderDistributionReady(t *testing.T, metadata broker.ClusterMetadata) {
	t.Helper()
	distribution := metadata.Raft.LeaderDistribution
	if distribution.GroupCount == 0 || distribution.ReadyGroups == 0 || len(distribution.Leaders) == 0 {
		t.Fatalf("expected ready leader distribution in cluster metadata: %#v", metadata)
	}
}

func fetchAllOrderRecordsFromAnyBroker(t *testing.T, brokers []*broker.Broker, expected int) {
	t.Helper()
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		for _, candidate := range brokers {
			if candidate == nil {
				continue
			}
			poll, err := candidate.Fetch(context.Background(), "c1", "orders", 0, nil, expected)
			if err == nil && len(poll.Records) >= expected {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("expected %d records from at least one broker", expected)
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
