package broker_test

import (
	"context"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerConsumerGroupSessionTimeoutRemovesMember(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)

	_, err := b.JoinConsumerGroup(ctx, "workers", "member-1", []string{"orders"}, 1)
	requireNoError(t, err)
	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, first, map[string]int{"member-1": 1, "member-2": 1})

	time.Sleep(5 * time.Millisecond)
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, second, map[string]int{"member-2": 2})
}

func TestBrokerConsumerGroupMaxPollTimeoutRemovesMember(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)

	_, err := b.JoinConsumerGroupWithOptions(ctx, "workers", "member-1", []string{"orders"}, broker.ConsumerGroupLeaseOptions{
		SessionTimeoutMS:  30_000,
		MaxPollIntervalMS: 1,
	})
	requireNoError(t, err)
	_, err = b.JoinConsumerGroup(ctx, "workers", "member-2", []string{"orders"}, 30_000)
	requireNoError(t, err)
	first, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, first, map[string]int{"member-1": 1, "member-2": 1})

	time.Sleep(5 * time.Millisecond)
	_, err = b.HeartbeatConsumerGroup(ctx, "workers", "member-1", nil)
	requireError(t, err)
	second, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	requireGroupLoads(t, second, map[string]int{"member-2": 2})
}

func TestBrokerConsumerGroupFetchRefreshesPollLease(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	_, err := b.JoinConsumerGroupWithOptions(ctx, "workers", "member-1", []string{"orders"}, broker.ConsumerGroupLeaseOptions{
		SessionTimeoutMS:  30_000,
		MaxPollIntervalMS: 30_000,
	})
	requireNoError(t, err)
	assignment, err := b.RebalanceConsumerGroup(ctx, "workers")
	requireNoError(t, err)
	before := groupMemberSnapshot(t, b, "workers", "member-1")

	time.Sleep(2 * time.Millisecond)
	_, err = b.FetchConsumerGroup(ctx, "workers", "member-1", assignment.Generation, "orders", 0, nil, 10)
	requireNoError(t, err)
	after := groupMemberSnapshot(t, b, "workers", "member-1")
	if after.LastPollMS <= before.LastPollMS {
		t.Fatalf("expected fetch to refresh last poll, before=%#v after=%#v", before, after)
	}
}

func groupMemberSnapshot(t *testing.T, b *broker.Broker, group, memberID string) broker.GroupMemberSummary {
	t.Helper()
	members, err := b.GroupMembersSnapshotFor(context.Background(), group)
	requireNoError(t, err)
	for _, member := range members {
		if member.MemberID == memberID {
			return member
		}
	}
	t.Fatalf("member %s not found in group %s: %#v", memberID, group, members)
	return broker.GroupMemberSummary{}
}
