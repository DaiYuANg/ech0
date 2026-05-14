package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) JoinConsumerGroup(ctx context.Context, group, memberID string, topics []string, sessionTimeoutMS uint64) (store.ConsumerGroupMember, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAlter, groupResource(identity, group)); err != nil {
		return store.ConsumerGroupMember{}, err
	}
	scopedTopics := collectionlist.NewListWithCapacity[string](len(topics))
	for _, topic := range topics {
		if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
			return store.ConsumerGroupMember{}, err
		}
		scopedTopics.Add(scopedTopicName(identity, topic))
	}
	req := joinGroupCommand{Group: scopedName(identity, "group", group), MemberID: memberID, Topics: scopedTopics.Values(), SessionTimeoutMS: sessionTimeoutMS}
	member, err := routeMetadataCommand(ctx, b, raftCommandJoinGroup, req, b.applyJoinGroup)
	if err != nil {
		return store.ConsumerGroupMember{}, err
	}
	return b.visibleGroupMember(identity, member), nil
}

func (b *Broker) HeartbeatConsumerGroup(ctx context.Context, group, memberID string, sessionTimeoutMS *uint64) (store.ConsumerGroupMember, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAlter, groupResource(identity, group)); err != nil {
		return store.ConsumerGroupMember{}, err
	}
	req := heartbeatGroupCommand{Group: scopedName(identity, "group", group), MemberID: memberID, SessionTimeoutMS: sessionTimeoutMS}
	member, err := routeMetadataCommand(ctx, b, raftCommandHeartbeatGroup, req, b.applyHeartbeatGroup)
	if err != nil {
		return store.ConsumerGroupMember{}, err
	}
	return b.visibleGroupMember(identity, member), nil
}

func (b *Broker) RebalanceConsumerGroup(ctx context.Context, group string) (store.ConsumerGroupAssignment, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAlter, groupResource(identity, group)); err != nil {
		return store.ConsumerGroupAssignment{}, err
	}
	req := rebalanceGroupCommand{Group: scopedName(identity, "group", group)}
	assignment, err := routeMetadataCommand(ctx, b, raftCommandRebalanceGroup, req, b.applyRebalanceGroup)
	if err != nil {
		return store.ConsumerGroupAssignment{}, err
	}
	return b.visibleGroupAssignment(identity, assignment), nil
}

func (b *Broker) GetConsumerGroupAssignment(group string) (*store.ConsumerGroupAssignment, error) {
	return b.GetConsumerGroupAssignmentFor(context.Background(), group)
}

func (b *Broker) GetConsumerGroupAssignmentFor(ctx context.Context, group string) (*store.ConsumerGroupAssignment, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionDescribe, groupResource(identity, group)); err != nil {
		return nil, err
	}
	scopedGroup := scopedName(identity, "group", group)
	assignment, err := b.meta.LoadGroupAssignment(scopedGroup)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group assignment")
	}
	if assignment != nil {
		visible := b.visibleGroupAssignment(identity, *assignment)
		assignment = &visible
	}
	return assignment, nil
}

func (b *Broker) FetchConsumerGroup(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32, offset *uint64, maxRecords int) (store.PollResult, error) {
	return b.FetchConsumerGroupWithIsolation(ctx, group, memberID, generation, topic, partition, offset, maxRecords, FetchIsolationReadUncommitted)
}

func (b *Broker) FetchConsumerGroupWithIsolation(
	ctx context.Context,
	group string,
	memberID string,
	generation uint64,
	topic string,
	partition uint32,
	offset *uint64,
	maxRecords int,
	isolation FetchIsolation,
) (store.PollResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, groupResource(identity, group)); err != nil {
		return store.PollResult{}, err
	}
	scopedGroup := scopedName(identity, "group", group)
	scopedTopic := scopedTopicName(identity, topic)
	if err := b.validateConsumerGroupLease(scopedGroup, memberID, generation, store.NewTopicPartition(scopedTopic, partition)); err != nil {
		return store.PollResult{}, err
	}
	return b.fetchWithIsolationScoped(ctx, groupConsumer(scopedGroup), scopedTopic, partition, offset, maxRecords, isolation)
}

func (b *Broker) CommitConsumerGroupOffset(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32, nextOffset uint64) error {
	return b.CommitConsumerGroupOffsetWithMetadata(ctx, group, memberID, generation, topic, partition, nextOffset, "")
}

func (b *Broker) CommitConsumerGroupOffsetWithMetadata(
	ctx context.Context,
	group string,
	memberID string,
	generation uint64,
	topic string,
	partition uint32,
	nextOffset uint64,
	metadata string,
) error {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, groupResource(identity, group)); err != nil {
		return err
	}
	scopedGroup := scopedName(identity, "group", group)
	scopedTopic := scopedTopicName(identity, topic)
	if err := b.validateConsumerGroupLease(scopedGroup, memberID, generation, store.NewTopicPartition(scopedTopic, partition)); err != nil {
		return err
	}
	req := commitOffsetCommand{
		Consumer:    groupConsumer(scopedGroup),
		Topic:       scopedTopic,
		Partition:   partition,
		NextOffset:  nextOffset,
		Metadata:    metadata,
		UpdatedAtMS: store.NowMS(),
	}
	return b.routeCommitOffset(ctx, req)
}

func (b *Broker) ConsumerGroupCommittedOffset(ctx context.Context, group, topic string, partition uint32) (*store.ConsumerOffsetState, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionDescribe, groupResource(identity, group)); err != nil {
		return nil, err
	}
	consumer := groupConsumer(scopedName(identity, "group", group))
	tp := store.NewTopicPartition(scopedTopicName(identity, topic), partition)
	state, err := b.meta.LoadConsumerOffsetState(consumer, tp)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group committed offset")
	}
	return visibleGroupOffsetState(identity, state), nil
}

func (b *Broker) visibleGroupMember(identity Identity, member store.ConsumerGroupMember) store.ConsumerGroupMember {
	member.Group = visibleName(identity, "group", member.Group)
	for index := range member.Topics {
		member.Topics[index] = visibleTopicName(identity, member.Topics[index])
	}
	return member
}

func (b *Broker) visibleGroupAssignment(identity Identity, assignment store.ConsumerGroupAssignment) store.ConsumerGroupAssignment {
	assignment.Group = visibleName(identity, "group", assignment.Group)
	for index := range assignment.Assignments {
		assignment.Assignments[index].Topic = visibleTopicName(identity, assignment.Assignments[index].Topic)
	}
	return assignment
}
