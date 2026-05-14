package broker_test

import (
	"context"
	"reflect"
	"testing"

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
}
