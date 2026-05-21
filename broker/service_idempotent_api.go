package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) PublishIdempotent(
	ctx context.Context,
	topic string,
	partitioning PublishPartitioning,
	key []byte,
	tombstone bool,
	payload []byte,
	idempotency ProduceIdempotency,
) (ProduceResult, error) {
	record := store.NewRecordAppend(payload)
	record.Key = append([]byte(nil), key...)
	if tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	return b.PublishRecordIdempotent(ctx, topic, partitioning, record, idempotency)
}

func (b *Broker) PublishRecordIdempotent(
	ctx context.Context,
	topic string,
	partitioning PublishPartitioning,
	record store.RecordAppend,
	idempotency ProduceIdempotency,
) (ProduceResult, error) {
	batch, err := b.PublishBatchIdempotent(ctx, topic, partitioning, []store.RecordAppend{record}, idempotency)
	if err != nil {
		return ProduceResult{}, err
	}
	if len(batch.Records) == 0 {
		return ProduceResult{}, brokerStoreError(store.CodeCodec, "idempotent produce returned no records")
	}
	return ProduceResult{Partition: batch.Partition, Record: batch.Records[0]}, nil
}

func (b *Broker) PublishBatchIdempotent(
	ctx context.Context,
	topic string,
	partitioning PublishPartitioning,
	records []store.RecordAppend,
	idempotency ProduceIdempotency,
) (ProduceBatchResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionProduce, topicResource(identity, topic)); err != nil {
		return ProduceBatchResult{}, err
	}
	if err := b.checkProduceQuota(ctx, identity, topic, len(records), batchPayloadBytes(records), batchStorageBytes(records)); err != nil {
		return ProduceBatchResult{}, err
	}
	return b.publishBatchScopedIdempotent(ctx, scopedTopicName(identity, topic), partitioning, records, idempotency)
}

func (b *Broker) publishBatchScopedIdempotent(
	ctx context.Context,
	topic string,
	partitioning PublishPartitioning,
	records []store.RecordAppend,
	idempotency ProduceIdempotency,
) (ProduceBatchResult, error) {
	copied := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for index := range records {
		record := records[index]
		applyPartitioningRoutingKey(&record, partitioning)
		copied.Add(cloneAppend(record))
	}
	req := produceBatchCommand{
		Topic:        topic,
		Partitioning: partitioning,
		Records:      copied.Values(),
		Idempotency:  &idempotency,
	}
	if b.usesClusterCommandRouter() {
		return b.proposeProduceBatchCoalesced(ctx, req)
	}
	return routePartitionCommand(ctx, b, publishPartitionCommandTarget(topic, partitioning), raftCommandProduceBatch, req, b.applyProduceBatch)
}
