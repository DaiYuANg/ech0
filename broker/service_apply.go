package broker

import (
	"cmp"
	"context"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionset "github.com/arcgolabs/collectionx/set"
)

func (b *Broker) applyCreateTopic(ctx context.Context, topic store.TopicConfig) (store.TopicConfig, error) {
	normalizeTopicPolicies(&topic)
	if isInternalTopicName(topic.Name) {
		return store.TopicConfig{}, brokerStoreError(store.CodeInvalidArgument, "topic name %s is reserved for internal broker use", topic.Name)
	}
	if err := b.queue.CreateTopic(topic); err != nil {
		return store.TopicConfig{}, wrapBroker("create_topic_failed", err, "create topic")
	}
	b.publishEvent(ctx, TopicCreatedEvent{Topic: topic.Name, Partitions: topic.Partitions})
	return topic, nil
}

func (b *Broker) applyProduce(ctx context.Context, req produceCommand) (ProduceResult, error) {
	topic, err := b.meta.LoadTopicConfig(req.Topic)
	if err != nil {
		return ProduceResult{}, wrapBrokerStore(err, "load topic config")
	}
	if topic == nil {
		return ProduceResult{}, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", req.Topic)
	}
	partition, err := b.router.selectPartition(*topic, req.Partitioning, req.Record.Key)
	if err != nil {
		return ProduceResult{}, err
	}
	record, err := b.queue.PublishRecord(req.Topic, partition, cloneAppend(req.Record))
	if err != nil {
		return ProduceResult{}, wrapBroker("publish_record_failed", err, "publish record")
	}
	b.metrics.RecordProduce(ctx, req.Partitioning.Mode, 1)
	b.publishEvent(ctx, RecordProducedEvent{Topic: req.Topic, Partition: partition, Offset: record.Offset, NextOffset: record.Offset + 1})
	return ProduceResult{Partition: partition, Record: record}, nil
}

func (b *Broker) applyProduceBatch(ctx context.Context, req produceBatchCommand) (ProduceBatchResult, error) {
	topic, err := b.meta.LoadTopicConfig(req.Topic)
	if err != nil {
		return ProduceBatchResult{}, wrapBrokerStore(err, "load topic config")
	}
	if topic == nil {
		return ProduceBatchResult{}, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", req.Topic)
	}
	if validateErr := b.validateBatchPayload(req.Records); validateErr != nil {
		return ProduceBatchResult{}, validateErr
	}
	partition, err := b.router.selectPartition(*topic, req.Partitioning, firstRecordKey(req.Records))
	if err != nil {
		return ProduceBatchResult{}, err
	}
	records, err := b.queue.PublishBatchRecords(req.Topic, partition, req.Records)
	if err != nil {
		return ProduceBatchResult{}, wrapBroker("publish_batch_failed", err, "publish record batch")
	}
	b.recordBatchProduce(ctx, req, partition, records)
	return ProduceBatchResult{Partition: partition, Records: records}, nil
}

func (b *Broker) validateBatchPayload(records []store.RecordAppend) error {
	totalBytes := 0
	for _, record := range records {
		totalBytes += len(record.Payload)
	}
	if totalBytes > b.cfg.Broker.MaxBatchPayloadBytes {
		return brokerStoreError(store.CodeInvalidArgument, "batch payload size %d exceeds limit %d", totalBytes, b.cfg.Broker.MaxBatchPayloadBytes)
	}
	return nil
}

func (b *Broker) recordBatchProduce(ctx context.Context, req produceBatchCommand, partition uint32, records []store.Record) {
	b.metrics.RecordProduce(ctx, req.Partitioning.Mode, uint64(len(records)))
	for _, record := range records {
		b.publishEvent(ctx, RecordProducedEvent{Topic: req.Topic, Partition: partition, Offset: record.Offset, NextOffset: record.Offset + 1})
	}
}

func (b *Broker) applyCommitOffset(ctx context.Context, req commitOffsetCommand) (struct{}, error) {
	_ = ctx
	return struct{}{}, wrapBroker("commit_offset_failed", b.queue.Ack(req.Consumer, req.Topic, req.Partition, req.NextOffset), "commit offset")
}

func (b *Broker) applyDirectSend(ctx context.Context, req directCommand) (direct.SendResult, error) {
	result, err := b.direct.Send(req.Sender, req.Recipient, req.ConversationID, req.Payload)
	if err != nil {
		return direct.SendResult{}, wrapBroker("direct_send_failed", err, "send direct message")
	}
	b.publishEvent(ctx, DirectMessageSentEvent{Sender: req.Sender, Recipient: req.Recipient, Offset: result.Offset})
	return result, nil
}

func (b *Broker) applyDirectAck(ctx context.Context, req ackDirectCommand) (struct{}, error) {
	_ = ctx
	return struct{}{}, wrapBroker("direct_ack_failed", b.direct.AckInbox(req.Recipient, req.NextOffset), "ack direct inbox")
}

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
	sessionTimeout := req.SessionTimeoutMS
	if sessionTimeout == 0 {
		sessionTimeout = 30000
	}
	return store.ConsumerGroupMember{
		Group:            req.Group,
		MemberID:         req.MemberID,
		Topics:           append([]string(nil), req.Topics...),
		SessionTimeoutMS: sessionTimeout,
		JoinedAtMS:       now,
		LastHeartbeatMS:  now,
	}
}

func (b *Broker) applyHeartbeatGroup(ctx context.Context, req heartbeatGroupCommand) (store.ConsumerGroupMember, error) {
	_ = ctx
	member, err := b.meta.LoadGroupMember(req.Group, req.MemberID)
	if err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "load group member")
	}
	if member == nil {
		return store.ConsumerGroupMember{}, brokerStoreError(store.CodeInvalidArgument, "group member %s/%s not found", req.Group, req.MemberID)
	}
	if req.SessionTimeoutMS != nil && *req.SessionTimeoutMS > 0 {
		member.SessionTimeoutMS = *req.SessionTimeoutMS
	}
	member.LastHeartbeatMS = store.NowMS()
	if err := b.meta.SaveGroupMember(*member); err != nil {
		return store.ConsumerGroupMember{}, wrapBrokerStore(err, "save group member heartbeat")
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
	out := make([]store.TopicPartition, 0)
	for i := range topics {
		topic := topics[i]
		if !wanted.Contains(topic.Name) {
			continue
		}
		for partition := range topic.Partitions {
			out = append(out, store.NewTopicPartition(topic.Name, partition))
		}
	}
	return collectionlist.NewList(out...).
		Sort(func(left, right store.TopicPartition) int {
			if left.Topic == right.Topic {
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).
		Values(), nil
}
