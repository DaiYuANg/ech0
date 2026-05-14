package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerGroupHealthSnapshotAggregatesGroupViews(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	assignment, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)

	health, err := b.GroupHealthSnapshotFor(ctx, "workers")
	requireNoError(t, err)
	requireGroupHealthCore(t, health, assignment.Generation)
	requireGroupHealthViews(t, health)
	requireGroupRebalanceHistory(t, health, assignment.Generation)
}

func requireGroupHealthCore(t *testing.T, health *broker.GroupHealthSummary, generation uint64) {
	t.Helper()
	if health.Status != "lagging" || health.Generation != generation {
		t.Fatalf("unexpected group health status: %#v", health)
	}
	if health.TotalMembers != 1 || health.ActiveMembers != 1 || health.AssignedPartitions != 1 {
		t.Fatalf("unexpected group health counts: %#v", health)
	}
	if health.TotalLagRecords != 1 || health.MaxPartitionLagRecords != 1 {
		t.Fatalf("unexpected group lag health: %#v", health)
	}
}

func requireGroupHealthViews(t *testing.T, health *broker.GroupHealthSummary) {
	t.Helper()
	if health.Assignment == nil || len(health.Assignment.Assignments) != 1 {
		t.Fatalf("expected assignment snapshot in health: %#v", health)
	}
	if health.Lag == nil || len(health.Lag.Partitions) != 1 {
		t.Fatalf("expected lag snapshot in health: %#v", health)
	}
}

func requireGroupRebalanceHistory(t *testing.T, health *broker.GroupHealthSummary, generation uint64) {
	t.Helper()
	if health.RebalanceExplain == nil || health.RebalanceExplain.NextGeneration != generation {
		t.Fatalf("expected rebalance explain in health: %#v", health)
	}
	if len(health.RebalanceHistory) != 1 || health.RebalanceHistory[0].Generation != generation {
		t.Fatalf("expected rebalance history in health: %#v", health)
	}
}
