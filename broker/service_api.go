package broker

import (
	"context"
	"time"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (b *Broker) CreateTopic(ctx context.Context, topic store.TopicConfig) (store.TopicConfig, error) {
	return proposeOrApply(ctx, b, raftCommandCreateTopic, topic, b.applyCreateTopic)
}

func (b *Broker) Publish(ctx context.Context, topic string, partitioning PublishPartitioning, key []byte, tombstone bool, payload []byte) (ProduceResult, error) {
	record := store.NewRecordAppend(payload)
	record.Key = append([]byte(nil), key...)
	if tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	return b.PublishRecord(ctx, topic, partitioning, record)
}

func (b *Broker) PublishRecord(ctx context.Context, topic string, partitioning PublishPartitioning, record store.RecordAppend) (ProduceResult, error) {
	if b.hasRaftNode() {
		batch, err := b.PublishBatch(ctx, topic, partitioning, []store.RecordAppend{record})
		if err != nil {
			return ProduceResult{}, err
		}
		if len(batch.Records) == 0 {
			return ProduceResult{}, brokerStoreError(store.CodeCodec, "raft produce batch returned no records")
		}
		return ProduceResult{Partition: batch.Partition, Record: batch.Records[0]}, nil
	}
	req := produceCommand{Topic: topic, Partitioning: partitioning, Record: cloneAppend(record)}
	return proposeOrApply(ctx, b, raftCommandProduce, req, b.applyProduce)
}

func (b *Broker) PublishBatch(ctx context.Context, topic string, partitioning PublishPartitioning, records []store.RecordAppend) (ProduceBatchResult, error) {
	copied := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for _, record := range records {
		copied.Add(cloneAppend(record))
	}
	req := produceBatchCommand{Topic: topic, Partitioning: partitioning, Records: copied.Values()}
	if b.hasRaftNode() {
		return b.proposeProduceBatchCoalesced(ctx, req)
	}
	return proposeOrApply(ctx, b, raftCommandProduceBatch, req, b.applyProduceBatch)
}

func (b *Broker) Fetch(ctx context.Context, consumer, topic string, partition uint32, offset *uint64, maxRecords int) (poll store.PollResult, err error) {
	const operation = "fetch"
	totalStart := time.Now()
	defer func() {
		b.recordFetchStage(ctx, operation, "total", len(poll.Records), totalStart, err)
	}()
	if maxRecords <= 0 || maxRecords > b.cfg.Broker.MaxFetchRecords {
		maxRecords = b.cfg.Broker.MaxFetchRecords
	}
	fetchStart := time.Now()
	poll, err = b.queue.Fetch(consumer, topic, partition, offset, maxRecords)
	b.recordFetchStage(ctx, operation, "queue_fetch", len(poll.Records), fetchStart, err)
	if err != nil {
		return store.PollResult{}, wrapBroker("queue_fetch_failed", err, "fetch records")
	}
	return poll, nil
}

func (b *Broker) CommitOffset(ctx context.Context, consumer, topic string, partition uint32, nextOffset uint64) error {
	req := commitOffsetCommand{Consumer: consumer, Topic: topic, Partition: partition, NextOffset: nextOffset}
	if b.hasRaftNode() {
		return b.proposeCommitOffsetCoalesced(ctx, req)
	}
	_, err := proposeOrApply(ctx, b, raftCommandCommitOffset, req, b.applyCommitOffset)
	return err
}

func (b *Broker) ListTopics() ([]store.TopicConfig, error) {
	topics, err := b.queue.ListTopics()
	if err != nil {
		return nil, wrapBroker("list_topics_failed", err, "list topics")
	}
	out := collectionlist.NewListWithCapacity[store.TopicConfig](len(topics))
	for i := range topics {
		topic := topics[i]
		if !isInternalTopicName(topic.Name) {
			out.Add(topic)
		}
	}
	return out.Values(), nil
}

func (b *Broker) SendDirect(ctx context.Context, sender, recipient string, conversationID *string, payload []byte) (direct.SendResult, error) {
	req := directCommand{Sender: sender, Recipient: recipient, ConversationID: conversationID, Payload: append([]byte(nil), payload...)}
	return proposeOrApply(ctx, b, raftCommandDirectSend, req, b.applyDirectSend)
}

func (b *Broker) FetchInbox(recipient string, maxRecords int) (direct.FetchInboxResult, error) {
	if maxRecords <= 0 || maxRecords > b.cfg.Broker.MaxFetchRecords {
		maxRecords = b.cfg.Broker.MaxFetchRecords
	}
	inbox, err := b.direct.FetchInbox(recipient, maxRecords)
	if err != nil {
		return direct.FetchInboxResult{}, wrapBroker("fetch_inbox_failed", err, "fetch inbox")
	}
	return inbox, nil
}

func (b *Broker) AckDirect(ctx context.Context, recipient string, nextOffset uint64) error {
	req := ackDirectCommand{Recipient: recipient, NextOffset: nextOffset}
	_, err := proposeOrApply(ctx, b, raftCommandDirectAck, req, b.applyDirectAck)
	return err
}

func (b *Broker) JoinConsumerGroup(ctx context.Context, group, memberID string, topics []string, sessionTimeoutMS uint64) (store.ConsumerGroupMember, error) {
	req := joinGroupCommand{Group: group, MemberID: memberID, Topics: collectionlist.NewList(topics...).Values(), SessionTimeoutMS: sessionTimeoutMS}
	return proposeOrApply(ctx, b, raftCommandJoinGroup, req, b.applyJoinGroup)
}

func (b *Broker) HeartbeatConsumerGroup(ctx context.Context, group, memberID string, sessionTimeoutMS *uint64) (store.ConsumerGroupMember, error) {
	req := heartbeatGroupCommand{Group: group, MemberID: memberID, SessionTimeoutMS: sessionTimeoutMS}
	return proposeOrApply(ctx, b, raftCommandHeartbeatGroup, req, b.applyHeartbeatGroup)
}

func (b *Broker) RebalanceConsumerGroup(ctx context.Context, group string) (store.ConsumerGroupAssignment, error) {
	req := rebalanceGroupCommand{Group: group}
	return proposeOrApply(ctx, b, raftCommandRebalanceGroup, req, b.applyRebalanceGroup)
}

func (b *Broker) GetConsumerGroupAssignment(group string) (*store.ConsumerGroupAssignment, error) {
	assignment, err := b.meta.LoadGroupAssignment(group)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group assignment")
	}
	return assignment, nil
}

func (b *Broker) FetchConsumerGroup(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32, offset *uint64, maxRecords int) (store.PollResult, error) {
	_ = memberID
	_ = generation
	return b.Fetch(ctx, groupConsumer(group), topic, partition, offset, maxRecords)
}

func (b *Broker) CommitConsumerGroupOffset(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32, nextOffset uint64) error {
	_ = memberID
	_ = generation
	return b.CommitOffset(ctx, groupConsumer(group), topic, partition, nextOffset)
}
