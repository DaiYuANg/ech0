package broker

import (
	"cmp"
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	defaultGroupSessionTimeoutMS    = 30000
	defaultGroupMaxPollIntervalMS   = 300000
	minGroupLeaseRefreshTimestampMS = 1
)

func (b *Broker) applyJoinGroup(ctx context.Context, req joinGroupCommand) (store.ConsumerGroupMember, error) {
	_ = ctx
	if req.Group == "" || req.MemberID == "" {
		return store.ConsumerGroupMember{}, brokerStoreError(store.CodeInvalidArgument, "group and member_id are required")
	}
	member := groupMemberFromCommand(req)
	if err := b.meta.SaveGroupMember(member); err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "save group member")
	}
	return member, nil
}

func groupMemberFromCommand(req joinGroupCommand) store.ConsumerGroupMember {
	now := store.NowMS()
	return store.ConsumerGroupMember{
		Group:             req.Group,
		MemberID:          req.MemberID,
		Topics:            collectionlist.NewList(req.Topics...).Values(),
		SessionTimeoutMS:  normalizeGroupSessionTimeout(req.SessionTimeoutMS),
		MaxPollIntervalMS: normalizeGroupMaxPollInterval(req.MaxPollIntervalMS),
		JoinedAtMS:        now,
		LastHeartbeatMS:   now,
		LastPollMS:        now,
	}
}

func normalizeGroupSessionTimeout(value uint64) uint64 {
	if value == 0 {
		return defaultGroupSessionTimeoutMS
	}
	return value
}

func normalizeGroupMaxPollInterval(value uint64) uint64 {
	if value == 0 {
		return defaultGroupMaxPollIntervalMS
	}
	return value
}

func (b *Broker) applyHeartbeatGroup(ctx context.Context, req heartbeatGroupCommand) (store.ConsumerGroupMember, error) {
	_ = ctx
	now := store.NowMS()
	member, err := b.loadUnexpiredGroupMember(req.Group, req.MemberID, now)
	if err != nil {
		return store.ConsumerGroupMember{}, err
	}
	if req.SessionTimeoutMS != nil && *req.SessionTimeoutMS > 0 {
		member.SessionTimeoutMS = *req.SessionTimeoutMS
	}
	if req.MaxPollIntervalMS != nil && *req.MaxPollIntervalMS > 0 {
		member.MaxPollIntervalMS = *req.MaxPollIntervalMS
	}
	member.LastHeartbeatMS = now
	if err := b.meta.SaveGroupMember(*member); err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "save group member heartbeat")
	}
	return *member, nil
}

func (b *Broker) applyPollGroup(ctx context.Context, req pollGroupCommand) (store.ConsumerGroupMember, error) {
	_ = ctx
	now := store.NowMS()
	member, err := b.loadUnexpiredGroupMember(req.Group, req.MemberID, now)
	if err != nil {
		return store.ConsumerGroupMember{}, err
	}
	member.LastPollMS = now
	if member.LastPollMS == 0 {
		member.LastPollMS = minGroupLeaseRefreshTimestampMS
	}
	if err := b.meta.SaveGroupMember(*member); err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "save group member poll")
	}
	return *member, nil
}

func (b *Broker) applyRebalanceGroup(ctx context.Context, req rebalanceGroupCommand) (store.ConsumerGroupAssignment, error) {
	now := store.NowMS()
	if _, err := b.meta.DeleteExpiredGroupMembers(now); err != nil {
		return store.ConsumerGroupAssignment{}, wrapBrokerStore(err, "delete expired group members")
	}
	active, err := b.activeGroupMembers(req.Group, now)
	if err != nil {
		return store.ConsumerGroupAssignment{}, err
	}
	previous, err := b.meta.LoadGroupAssignment(req.Group)
	if err != nil {
		return store.ConsumerGroupAssignment{}, wrapBrokerStore(err, "load group assignment")
	}
	plan, err := b.buildGroupRebalancePlan(req.Group, now, previous, active)
	if err != nil {
		return store.ConsumerGroupAssignment{}, err
	}
	if err := b.meta.SaveGroupAssignment(plan.Assignment); err != nil {
		return store.ConsumerGroupAssignment{}, wrapBrokerStore(err, "save group assignment")
	}
	recordRebalanceMetrics(ctx, b.metrics, plan)
	b.recordGroupRebalanceHistory(plan)
	return plan.Assignment, nil
}

func (b *Broker) groupPartitions(members []store.ConsumerGroupMember) ([]store.TopicPartition, error) {
	wanted := collectionset.NewSet[string]()
	for _, member := range members {
		wanted.Add(member.Topics...)
	}
	topics, err := b.meta.ListTopics()
	if err != nil {
		return nil, wrapBrokerStore(err, "list topics for group partitions")
	}
	out := collectionlist.NewList[store.TopicPartition]()
	for i := range topics {
		topic := topics[i]
		if !wanted.Contains(topic.Name) {
			continue
		}
		for partition := range topic.Partitions {
			out.Add(store.NewTopicPartition(topic.Name, partition))
		}
	}
	return out.Sort(compareTopicPartitions).Values(), nil
}

func compareTopicPartitions(left, right store.TopicPartition) int {
	if left.Topic == right.Topic {
		return cmp.Compare(left.Partition, right.Partition)
	}
	return cmp.Compare(left.Topic, right.Topic)
}
