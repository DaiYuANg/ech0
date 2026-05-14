package broker_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerStaticMemberRejoinKeepsAssignmentGeneration(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	if first.Generation == 0 || len(first.Assignments) != 2 {
		t.Fatalf("unexpected first assignment: %#v", first)
	}

	_, err = b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	if second.Generation != first.Generation || !reflect.DeepEqual(second.Assignments, first.Assignments) {
		t.Fatalf("expected static member rejoin to keep assignment, first=%#v second=%#v", first, second)
	}
}

func TestBrokerNewGroupMemberAdvancesAssignmentGeneration(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)

	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	if second.Generation != first.Generation+1 {
		t.Fatalf("expected new member to advance generation, first=%#v second=%#v", first, second)
	}
	requireGroupLoads(t, second, map[string]int{"member-1": 1, "member-2": 1})
}

func TestBrokerCooperativeStickyRebalanceMovesMinimumPartitionsForNewMember(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 6
	createTopic(ctx, t, b, topic)

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, first, map[string]int{"member-1": 3, "member-2": 3})

	_, err = b.JoinConsumerGroup(ctx, "workers", "member-3", []string{"orders"}, 30_000)
	requireNoError(t, err)
	explain, err := b.GroupRebalanceExplainFor(ctx, "workers")
	requireNoError(t, err)
	if explain.MovedPartitions != 2 || explain.StickyApplied != 4 {
		t.Fatalf("unexpected cooperative-sticky explain: %#v", explain)
	}
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, second, map[string]int{"member-1": 2, "member-2": 2, "member-3": 2})
}

func TestBrokerCooperativeStickyRebalanceKeepsRemainingMembersBalancedAfterLeave(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 6
	createTopic(ctx, t, b, topic)

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 30_000)
	requireNoError(t, err)
	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	_, err = b.JoinConsumerGroup(ctx, "workers", "member-3", []string{"orders"}, 1)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, first, map[string]int{"member-1": 2, "member-2": 2, "member-3": 2})

	time.Sleep(2 * time.Millisecond)
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, second, map[string]int{"member-1": 3, "member-2": 3})
	if moved := movedAssignmentCount(first, second); moved != 2 {
		t.Fatalf("expected only expired member partitions to move, moved=%d first=%#v second=%#v", moved, first, second)
	}
}

func requireGroupLoads(t *testing.T, assignment store.ConsumerGroupAssignment, expected map[string]int) {
	t.Helper()
	loads := map[string]int{}
	for _, item := range assignment.Assignments {
		loads[item.MemberID]++
	}
	if !reflect.DeepEqual(loads, expected) {
		t.Fatalf("unexpected assignment loads: got %#v want %#v assignment=%#v", loads, expected, assignment)
	}
}

func movedAssignmentCount(previous, next store.ConsumerGroupAssignment) int {
	owners := map[store.TopicPartition]string{}
	for _, item := range previous.Assignments {
		owners[store.NewTopicPartition(item.Topic, item.Partition)] = item.MemberID
	}
	moved := 0
	for _, item := range next.Assignments {
		if owners[store.NewTopicPartition(item.Topic, item.Partition)] != item.MemberID {
			moved++
		}
	}
	return moved
}
