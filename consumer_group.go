package ech0

import (
	"cmp"
	"context"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
)

type TopicPartition struct {
	Topic     string
	Partition uint32
}

type ConsumerGroup struct {
	broker         *Broker
	group          string
	memberID       string
	topics         []string
	sessionTimeout time.Duration
	callbacks      ConsumerGroupCallbacks

	generation  uint64
	assignments []TopicPartition
}

type ConsumerGroupCallbacks struct {
	OnRevoke func(context.Context, ConsumerGroupRebalance) error
	OnAssign func(context.Context, ConsumerGroupRebalance) error
}

type ConsumerGroupRebalance struct {
	Group       string
	MemberID    string
	Generation  uint64
	Assignments []TopicPartition
	Revoked     []TopicPartition
	Assigned    []TopicPartition
}

type ConsumerGroupOption func(*consumerGroupOptions)

type consumerGroupOptions struct {
	sessionTimeout time.Duration
	callbacks      ConsumerGroupCallbacks
}

func WithConsumerGroupCallbacks(callbacks ConsumerGroupCallbacks) ConsumerGroupOption {
	return func(opts *consumerGroupOptions) {
		opts.callbacks = callbacks
	}
}

func ConsumerGroupSessionTimeout(timeout time.Duration) ConsumerGroupOption {
	return func(opts *consumerGroupOptions) {
		if timeout > 0 {
			opts.sessionTimeout = timeout
		}
	}
}

func (b *Broker) JoinConsumerGroup(ctx context.Context, group, memberID string, topics []string, opts ...ConsumerGroupOption) (*ConsumerGroup, error) {
	options := normalizeConsumerGroupOptions(opts)
	joinedTopics := collectionlist.NewList(topics...).Values()
	_, err := b.broker.JoinConsumerGroup(ctx, group, memberID, joinedTopics, durationMillis(options.sessionTimeout))
	if err != nil {
		return nil, oops.In("embedded").Code("consumer_group_join_failed").With("group", group, "member_id", memberID).Wrapf(err, "join consumer group")
	}
	session := &ConsumerGroup{
		broker:         b,
		group:          group,
		memberID:       memberID,
		topics:         joinedTopics,
		sessionTimeout: options.sessionTimeout,
		callbacks:      options.callbacks,
	}
	if _, err := session.Rebalance(ctx); err != nil {
		return nil, err
	}
	return session, nil
}

func normalizeConsumerGroupOptions(opts []ConsumerGroupOption) consumerGroupOptions {
	options := consumerGroupOptions{sessionTimeout: 30 * time.Second}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	return options
}

func (g *ConsumerGroup) Rebalance(ctx context.Context) (ConsumerGroupRebalance, error) {
	if err := g.ensureReady(); err != nil {
		return ConsumerGroupRebalance{}, err
	}
	assignment, err := g.broker.broker.RebalanceConsumerGroup(ctx, g.group)
	if err != nil {
		return ConsumerGroupRebalance{}, oops.In("embedded").Code("consumer_group_rebalance_failed").With("group", g.group).Wrapf(err, "rebalance consumer group")
	}
	next := groupOwnedTopicPartitions(assignment, g.memberID)
	change := g.rebalanceChange(assignment.Generation, next)
	if err := g.runRevokeCallback(ctx, change); err != nil {
		return ConsumerGroupRebalance{}, err
	}
	g.generation = assignment.Generation
	g.assignments = next
	if err := g.runAssignCallback(ctx, change); err != nil {
		return ConsumerGroupRebalance{}, err
	}
	return change, nil
}

func (g *ConsumerGroup) Heartbeat(ctx context.Context) error {
	if err := g.ensureReady(); err != nil {
		return err
	}
	var timeoutMS *uint64
	if g.sessionTimeout > 0 {
		value := durationMillis(g.sessionTimeout)
		timeoutMS = &value
	}
	_, err := g.broker.broker.HeartbeatConsumerGroup(ctx, g.group, g.memberID, timeoutMS)
	return oops.In("embedded").Code("consumer_group_heartbeat_failed").With("group", g.group, "member_id", g.memberID).Wrapf(err, "heartbeat consumer group")
}

func (g *ConsumerGroup) Fetch(ctx context.Context, topic string, partition uint32, opts ...FetchOption) (FetchResult, error) {
	if err := g.ensureReady(); err != nil {
		return FetchResult{}, err
	}
	fetchOpts := fetchOptions{maxRecords: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&fetchOpts)
		}
	}
	poll, err := g.broker.broker.FetchConsumerGroupWithIsolation(ctx, g.group, g.memberID, g.generation, topic, partition, fetchOpts.offset, fetchOpts.maxRecords, fetchOpts.isolation)
	if err != nil {
		return FetchResult{}, oops.In("embedded").Code("consumer_group_fetch_failed").With("group", g.group, "member_id", g.memberID, "topic", topic).Wrapf(err, "fetch consumer group messages")
	}
	messages := make([]Message, 0, len(poll.Records))
	for _, record := range poll.Records {
		messages = append(messages, messageFromRecord(topic, partition, record))
	}
	return FetchResult{Messages: messages, NextOffset: poll.NextOffset, HighWatermark: poll.HighWatermark}, nil
}

func (g *ConsumerGroup) Ack(ctx context.Context, msg Message) error {
	return g.CommitWithMetadata(ctx, msg.Topic, msg.Partition, msg.NextOffset, "")
}

func (g *ConsumerGroup) AckWithMetadata(ctx context.Context, msg Message, metadata string) error {
	return g.CommitWithMetadata(ctx, msg.Topic, msg.Partition, msg.NextOffset, metadata)
}

func (g *ConsumerGroup) Commit(ctx context.Context, topic string, partition uint32, nextOffset uint64) error {
	return g.CommitWithMetadata(ctx, topic, partition, nextOffset, "")
}

func (g *ConsumerGroup) CommitWithMetadata(ctx context.Context, topic string, partition uint32, nextOffset uint64, metadata string) error {
	if err := g.ensureReady(); err != nil {
		return err
	}
	err := g.broker.broker.CommitConsumerGroupOffsetWithMetadata(ctx, g.group, g.memberID, g.generation, topic, partition, nextOffset, metadata)
	return oops.In("embedded").Code("consumer_group_commit_failed").With("group", g.group, "member_id", g.memberID, "topic", topic).Wrapf(err, "commit consumer group offset")
}

func (g *ConsumerGroup) CommittedOffset(ctx context.Context, topic string, partition uint32) (*CommittedOffset, error) {
	if err := g.ensureReady(); err != nil {
		return nil, err
	}
	state, err := g.broker.broker.ConsumerGroupCommittedOffset(ctx, g.group, topic, partition)
	if err != nil {
		return nil, oops.In("embedded").Code("consumer_group_committed_offset_failed").With("group", g.group, "topic", topic).Wrapf(err, "load consumer group committed offset")
	}
	return committedOffsetFromStore(state), nil
}

func (g *ConsumerGroup) Assignments() ConsumerGroupRebalance {
	if g == nil {
		return ConsumerGroupRebalance{}
	}
	return ConsumerGroupRebalance{
		Group:       g.group,
		MemberID:    g.memberID,
		Generation:  g.generation,
		Assignments: cloneTopicPartitions(g.assignments),
	}
}

func (g *ConsumerGroup) rebalanceChange(generation uint64, next []TopicPartition) ConsumerGroupRebalance {
	previous := cloneTopicPartitions(g.assignments)
	return ConsumerGroupRebalance{
		Group:       g.group,
		MemberID:    g.memberID,
		Generation:  generation,
		Assignments: cloneTopicPartitions(next),
		Revoked:     partitionDiff(previous, next),
		Assigned:    partitionDiff(next, previous),
	}
}

func (g *ConsumerGroup) runRevokeCallback(ctx context.Context, change ConsumerGroupRebalance) error {
	if g.callbacks.OnRevoke == nil || len(change.Revoked) == 0 {
		return nil
	}
	event := change
	event.Assignments = nil
	event.Assigned = nil
	if err := g.callbacks.OnRevoke(ctx, event); err != nil {
		return oops.In("embedded").Code("consumer_group_revoke_failed").With("group", g.group, "member_id", g.memberID).Wrapf(err, "revoke consumer group partitions")
	}
	return nil
}

func (g *ConsumerGroup) runAssignCallback(ctx context.Context, change ConsumerGroupRebalance) error {
	if g.callbacks.OnAssign == nil || len(change.Assigned) == 0 {
		return nil
	}
	event := change
	event.Revoked = nil
	if err := g.callbacks.OnAssign(ctx, event); err != nil {
		return oops.In("embedded").Code("consumer_group_assign_failed").With("group", g.group, "member_id", g.memberID).Wrapf(err, "assign consumer group partitions")
	}
	return nil
}

func (g *ConsumerGroup) ensureReady() error {
	if g == nil || g.broker == nil || g.broker.broker == nil {
		return oops.In("embedded").Code("consumer_group_nil").Wrap(store.E(store.CodeInvalidArgument, "consumer group is nil"))
	}
	return nil
}

func groupOwnedTopicPartitions(assignment store.ConsumerGroupAssignment, memberID string) []TopicPartition {
	out := collectionlist.NewList[TopicPartition]()
	for _, item := range assignment.Assignments {
		if item.MemberID == memberID {
			out.Add(TopicPartition{Topic: item.Topic, Partition: item.Partition})
		}
	}
	return sortTopicPartitions(out.Values())
}

type topicPartitionKey struct {
	topic     string
	partition uint32
}

func partitionDiff(left, right []TopicPartition) []TopicPartition {
	rightIndex := topicPartitionIndex(right)
	out := collectionlist.NewList[TopicPartition]()
	for _, item := range left {
		if _, ok := rightIndex.Get(topicPartitionKey{topic: item.Topic, partition: item.Partition}); !ok {
			out.Add(item)
		}
	}
	return sortTopicPartitions(out.Values())
}

func topicPartitionIndex(partitions []TopicPartition) *collectionmapping.Map[topicPartitionKey, TopicPartition] {
	index := collectionmapping.NewMapWithCapacity[topicPartitionKey, TopicPartition](len(partitions))
	for _, item := range partitions {
		index.Set(topicPartitionKey{topic: item.Topic, partition: item.Partition}, item)
	}
	return index
}

func cloneTopicPartitions(partitions []TopicPartition) []TopicPartition {
	return collectionlist.NewList(partitions...).Values()
}

func sortTopicPartitions(partitions []TopicPartition) []TopicPartition {
	return collectionlist.NewList(partitions...).
		Sort(func(left, right TopicPartition) int {
			if left.Topic == right.Topic {
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).Values()
}
