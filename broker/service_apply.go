package broker

import (
	"cmp"
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) applyCreateTopic(ctx context.Context, topic store.TopicConfig) (store.TopicConfig, error) {
	normalizeTopicPolicies(&topic)
	if isInternalTopicName(topic.Name) {
		return store.TopicConfig{}, brokerStoreError(store.CodeInvalidArgument, "topic name %s is reserved for internal broker use", topic.Name)
	}
	if err := b.queue.CreateTopic(topic); err != nil {
		return store.TopicConfig{}, wrapBroker("create_topic_failed", err, "create topic")
	}
	if err := b.ensureTopicShardPlacements(topic); err != nil {
		return store.TopicConfig{}, wrapBroker("topic_shard_placement_failed", err, "ensure topic shard placement")
	}
	if err := b.ensureTopicDataShards(ctx, topic); err != nil {
		return store.TopicConfig{}, err
	}
	b.cacheTopicConfig(topic)
	b.publishEvent(ctx, TopicCreatedEvent{Topic: topic.Name, Partitions: topic.Partitions})
	return topic, nil
}

func (b *Broker) applyProduce(ctx context.Context, req produceCommand) (ProduceResult, error) {
	if req.Idempotency != nil {
		batch, err := b.applyProduceBatch(ctx, produceBatchCommand{
			Topic:        req.Topic,
			Partitioning: req.Partitioning,
			Records:      []store.RecordAppend{req.Record},
			Idempotency:  cloneProduceIdempotency(req.Idempotency),
		})
		if err != nil {
			return ProduceResult{}, err
		}
		if len(batch.Records) == 0 {
			return ProduceResult{}, brokerStoreError(store.CodeCodec, "idempotent produce returned no records")
		}
		return ProduceResult{Partition: batch.Partition, Record: batch.Records[0]}, nil
	}
	topic, err := b.topicConfig(req.Topic)
	if err != nil {
		return ProduceResult{}, err
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
	item, err := b.applyProduceBatchInternal(ctx, req)
	if err != nil {
		return ProduceBatchResult{}, err
	}
	if item.Appended {
		b.recordBatchProduce(ctx, req, item.Result.Partition, item.Result.Records)
	}
	return item.Result, nil
}

func (b *Broker) applyProduceBatchInternal(_ context.Context, req produceBatchCommand) (produceBatchItemResult, error) {
	topic, err := b.topicConfig(req.Topic)
	if err != nil {
		return produceBatchItemResult{}, err
	}
	if topic == nil {
		return produceBatchItemResult{}, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", req.Topic)
	}
	if validateErr := b.validateBatchPayload(req.Records); validateErr != nil {
		return produceBatchItemResult{}, validateErr
	}
	partition, err := b.router.selectPartition(*topic, req.Partitioning, firstRecordKey(req.Records))
	if err != nil {
		return produceBatchItemResult{}, err
	}
	appended, err := b.appendProduceBatch(req.Topic, partition, req)
	if err != nil {
		return produceBatchItemResult{}, err
	}
	return produceBatchItemResult{
		Result:   ProduceBatchResult{Partition: partition, Records: appended.Records},
		Appended: appended.Appended,
	}, nil
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
	state := store.ConsumerOffsetState{
		Consumer:    req.Consumer,
		Topic:       req.Topic,
		Partition:   req.Partition,
		NextOffset:  req.NextOffset,
		Metadata:    req.Metadata,
		UpdatedAtMS: req.UpdatedAtMS,
	}
	if state.UpdatedAtMS == 0 {
		state.UpdatedAtMS = store.NowMS()
	}
	return struct{}{}, wrapBrokerStore(b.meta.SaveConsumerOffsetState(state), "commit offset")
}

type commitOffsetApplyKey struct {
	consumer  string
	topic     string
	partition uint32
}

func (b *Broker) applyCommitOffsets(ctx context.Context, req commitOffsetsCommand) (commitOffsetsResult, error) {
	compacted := compactCommitOffsetCommands(req.Requests)
	errorsByKey := collectionmapping.NewMap[commitOffsetApplyKey, string]()
	compacted.Range(func(key commitOffsetApplyKey, command commitOffsetCommand) bool {
		_, err := b.applyCommitOffset(ctx, command)
		if err != nil {
			errorsByKey.Set(key, err.Error())
		}
		return true
	})

	items := collectionlist.NewListWithCapacity[commitOffsetItemResult](len(req.Requests))
	for _, command := range req.Requests {
		items.Add(commitOffsetItemResult{Error: errorsByKey.GetOrDefault(commitOffsetKey(command), "")})
	}
	return commitOffsetsResult{Items: items.Values()}, nil
}

func compactCommitOffsetCommands(commands []commitOffsetCommand) *collectionmapping.Map[commitOffsetApplyKey, commitOffsetCommand] {
	compacted := collectionmapping.NewMapWithCapacity[commitOffsetApplyKey, commitOffsetCommand](len(commands))
	for _, command := range commands {
		key := commitOffsetKey(command)
		existing, ok := compacted.Get(key)
		if !ok || command.NextOffset >= existing.NextOffset {
			compacted.Set(key, command)
		}
	}
	return compacted
}

func commitOffsetKey(command commitOffsetCommand) commitOffsetApplyKey {
	return commitOffsetApplyKey{consumer: command.Consumer, topic: command.Topic, partition: command.Partition}
}

func errorMessage(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
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
		Topics:           collectionlist.NewList(req.Topics...).Values(),
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
	return out.
		Sort(func(left, right store.TopicPartition) int {
			if left.Topic == right.Topic {
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).
		Values(), nil
}
