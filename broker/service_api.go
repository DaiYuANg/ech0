package broker

import (
	"context"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/direct"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) CreateTopic(ctx context.Context, topic store.TopicConfig) (store.TopicConfig, error) {
	identity, scopedTopic := b.scopedTopicConfig(ctx, topic)
	if err := b.authorize(ctx, identity, ACLActionCreate, topicResource(identity, topic.Name)); err != nil {
		return store.TopicConfig{}, err
	}
	if err := b.checkQuota(ctx, QuotaRequest{
		Identity:   identity,
		Action:     QuotaActionCreateTopic,
		Topic:      topic.Name,
		Partitions: topic.Partitions,
	}); err != nil {
		return store.TopicConfig{}, err
	}
	created, err := routeMetadataCommand(ctx, b, raftCommandCreateTopic, scopedTopic, b.applyCreateTopic)
	if err != nil {
		return store.TopicConfig{}, err
	}
	return b.visibleTopicConfig(identity, created), nil
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
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionProduce, topicResource(identity, topic)); err != nil {
		return ProduceResult{}, err
	}
	if err := b.checkQuota(ctx, QuotaRequest{Identity: identity, Action: QuotaActionProduce, Topic: topic, Records: 1, Bytes: len(record.Payload)}); err != nil {
		return ProduceResult{}, err
	}
	return b.publishRecordScoped(ctx, scopedTopicName(identity, topic), partitioning, record)
}

func (b *Broker) publishRecordScoped(ctx context.Context, topic string, partitioning PublishPartitioning, record store.RecordAppend) (ProduceResult, error) {
	if b.usesClusterCommandRouter() {
		batch, err := b.publishBatchScoped(ctx, topic, partitioning, []store.RecordAppend{record})
		if err != nil {
			return ProduceResult{}, err
		}
		if len(batch.Records) == 0 {
			return ProduceResult{}, brokerStoreError(store.CodeCodec, "raft produce batch returned no records")
		}
		return ProduceResult{Partition: batch.Partition, Record: batch.Records[0]}, nil
	}
	req := produceCommand{Topic: topic, Partitioning: partitioning, Record: cloneAppend(record)}
	return routePartitionCommand(ctx, b, publishPartitionCommandTarget(topic, partitioning), raftCommandProduce, req, b.applyProduce)
}

func (b *Broker) PublishBatch(ctx context.Context, topic string, partitioning PublishPartitioning, records []store.RecordAppend) (ProduceBatchResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionProduce, topicResource(identity, topic)); err != nil {
		return ProduceBatchResult{}, err
	}
	if err := b.checkQuota(ctx, QuotaRequest{
		Identity: identity,
		Action:   QuotaActionProduce,
		Topic:    topic,
		Records:  len(records),
		Bytes:    batchPayloadBytes(records),
	}); err != nil {
		return ProduceBatchResult{}, err
	}
	return b.publishBatchScoped(ctx, scopedTopicName(identity, topic), partitioning, records)
}

func (b *Broker) publishBatchScoped(ctx context.Context, topic string, partitioning PublishPartitioning, records []store.RecordAppend) (ProduceBatchResult, error) {
	copied := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for _, record := range records {
		copied.Add(cloneAppend(record))
	}
	req := produceBatchCommand{Topic: topic, Partitioning: partitioning, Records: copied.Values()}
	if b.usesClusterCommandRouter() {
		return b.proposeProduceBatchCoalesced(ctx, req)
	}
	return routePartitionCommand(ctx, b, publishPartitionCommandTarget(topic, partitioning), raftCommandProduceBatch, req, b.applyProduceBatch)
}

func (b *Broker) publishBatches(ctx context.Context, requests []produceBatchCommand) (produceBatchesResult, error) {
	identity := b.identity(ctx)
	scopedRequests := collectionlist.NewListWithCapacity[produceBatchCommand](len(requests))
	for _, request := range requests {
		if request.Topic != "" {
			if err := b.authorize(ctx, identity, ACLActionProduce, topicResource(identity, request.Topic)); err != nil {
				return produceBatchesResult{}, err
			}
			if err := b.checkQuota(ctx, QuotaRequest{
				Identity: identity,
				Action:   QuotaActionProduce,
				Topic:    request.Topic,
				Records:  len(request.Records),
				Bytes:    batchPayloadBytes(request.Records),
			}); err != nil {
				return produceBatchesResult{}, err
			}
			request.Topic = scopedTopicName(identity, request.Topic)
		}
		scopedRequests.Add(request)
	}
	requests = scopedRequests.Values()
	req := produceBatchesCommand{Requests: requests}
	if b.usesClusterCommandRouter() {
		return b.routeProduceBatchesCommand(ctx, req)
	}
	return b.applyProduceBatches(ctx, req)
}

func (b *Broker) Fetch(ctx context.Context, consumer, topic string, partition uint32, offset *uint64, maxRecords int) (poll store.PollResult, err error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
		return store.PollResult{}, err
	}
	if err := b.checkQuota(ctx, QuotaRequest{Identity: identity, Action: QuotaActionConsume, Topic: topic, Records: maxRecords}); err != nil {
		return store.PollResult{}, err
	}
	return b.fetchScoped(ctx, scopedName(identity, "consumer", consumer), scopedTopicName(identity, topic), partition, offset, maxRecords)
}

func (b *Broker) fetchScoped(ctx context.Context, consumer, topic string, partition uint32, offset *uint64, maxRecords int) (poll store.PollResult, err error) {
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
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, topicResource(identity, topic)); err != nil {
		return err
	}
	req := commitOffsetCommand{Consumer: scopedName(identity, "consumer", consumer), Topic: scopedTopicName(identity, topic), Partition: partition, NextOffset: nextOffset}
	if b.usesClusterCommandRouter() {
		return b.proposeCommitOffsetCoalesced(ctx, req)
	}
	_, err := routePartitionCommand(ctx, b, exactPartitionCommandTarget(req.Topic, partition), raftCommandCommitOffset, req, b.applyCommitOffset)
	return err
}

func (b *Broker) ListTopics() ([]store.TopicConfig, error) {
	return b.ListTopicsFor(context.Background())
}

func (b *Broker) ListTopicsFor(ctx context.Context) ([]store.TopicConfig, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionDescribe, ACLResource{Type: ACLResourceNamespace, Tenant: identity.Tenant, Namespace: identity.Namespace, Name: identity.Namespace}); err != nil {
		return nil, err
	}
	topics, err := b.queue.ListTopics()
	if err != nil {
		return nil, wrapBroker("list_topics_failed", err, "list topics")
	}
	out := collectionlist.NewListWithCapacity[store.TopicConfig](len(topics))
	for i := range topics {
		topic := topics[i]
		if nameInScope(identity, "topic", topic.Name) && !isInternalTopicName(visibleName(identity, "topic", topic.Name)) {
			out.Add(b.visibleTopicConfig(identity, topic))
		}
	}
	return out.Values(), nil
}

func (b *Broker) SendDirect(ctx context.Context, sender, recipient string, conversationID *string, payload []byte) (direct.SendResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionProduce, directInboxResource(identity, recipient)); err != nil {
		return direct.SendResult{}, err
	}
	req := directCommand{
		Sender:         scopedName(identity, "direct", sender),
		Recipient:      scopedName(identity, "direct", recipient),
		ConversationID: conversationID,
		Payload:        append([]byte(nil), payload...),
	}
	return routePartitionCommand(ctx, b, topicCommandTarget(direct.InternalInboxTopicPrefix), raftCommandDirectSend, req, b.applyDirectSend)
}

func (b *Broker) FetchInbox(recipient string, maxRecords int) (direct.FetchInboxResult, error) {
	return b.FetchInboxFor(context.Background(), recipient, maxRecords)
}

func (b *Broker) FetchInboxFor(ctx context.Context, recipient string, maxRecords int) (direct.FetchInboxResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, directInboxResource(identity, recipient)); err != nil {
		return direct.FetchInboxResult{}, err
	}
	if maxRecords <= 0 || maxRecords > b.cfg.Broker.MaxFetchRecords {
		maxRecords = b.cfg.Broker.MaxFetchRecords
	}
	scopedRecipient := scopedName(identity, "direct", recipient)
	inbox, err := b.direct.FetchInbox(scopedRecipient, maxRecords)
	if err != nil {
		return direct.FetchInboxResult{}, wrapBroker("fetch_inbox_failed", err, "fetch inbox")
	}
	inbox.Recipient = recipient
	for index := range inbox.Records {
		message := &inbox.Records[index].Message
		message.Sender = visibleName(identity, "direct", message.Sender)
		message.Recipient = visibleName(identity, "direct", message.Recipient)
	}
	return inbox, nil
}

func (b *Broker) AckDirect(ctx context.Context, recipient string, nextOffset uint64) error {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, directInboxResource(identity, recipient)); err != nil {
		return err
	}
	req := ackDirectCommand{Recipient: scopedName(identity, "direct", recipient), NextOffset: nextOffset}
	_, err := routePartitionCommand(ctx, b, topicCommandTarget(direct.InternalInboxTopicPrefix), raftCommandDirectAck, req, b.applyDirectAck)
	return err
}

func batchPayloadBytes(records []store.RecordAppend) int {
	total := 0
	for _, record := range records {
		total += len(record.Payload)
	}
	return total
}
