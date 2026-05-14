package broker

import (
	"context"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) BeginTransaction(ctx context.Context, transactionalID string, timeoutMS uint64) (TransactionBeginResult, error) {
	req := txBeginCommand{TransactionalID: strings.TrimSpace(transactionalID), TimeoutMS: timeoutMS}
	return routeMetadataCommand(ctx, b, raftCommandTxBegin, req, b.applyTxBegin)
}

func (b *Broker) PublishTransactionalRecord(
	ctx context.Context,
	identity TransactionIdentity,
	sequence uint64,
	topic string,
	partitioning PublishPartitioning,
	record store.RecordAppend,
) (TransactionPublishResult, error) {
	partition, err := b.selectTransactionPartition(topic, partitioning, record.Key)
	if err != nil {
		return TransactionPublishResult{}, err
	}
	req := txPublishCommand{
		Identity:  identity,
		Sequence:  sequence,
		Topic:     topic,
		Partition: partition,
		Record:    cloneAppend(record),
	}
	return routeMetadataCommand(ctx, b, raftCommandTxPublish, req, b.applyTxPublish)
}

func (b *Broker) PublishTransactionalBatch(
	ctx context.Context,
	identity TransactionIdentity,
	baseSequence uint64,
	topic string,
	partitioning PublishPartitioning,
	records []store.RecordAppend,
) (TransactionPublishBatchResult, error) {
	copied := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for _, record := range records {
		copied.Add(cloneAppend(record))
	}
	partition, err := b.selectTransactionPartition(topic, partitioning, firstRecordKey(copied.Values()))
	if err != nil {
		return TransactionPublishBatchResult{}, err
	}
	req := txPublishBatchCommand{
		Identity:     identity,
		BaseSequence: baseSequence,
		Topic:        topic,
		Partition:    partition,
		Records:      copied.Values(),
	}
	return routeMetadataCommand(ctx, b, raftCommandTxPublishBatch, req, b.applyTxPublishBatch)
}

func (b *Broker) CommitTransactionOffset(
	ctx context.Context,
	identity TransactionIdentity,
	offset TransactionOffsetCommit,
) (TransactionOffsetCommitResult, error) {
	req := txCommitOffsetCommand{
		Identity:   identity,
		Consumer:   offset.Consumer,
		Group:      offset.Group,
		MemberID:   offset.MemberID,
		Generation: offset.Generation,
		Topic:      offset.Topic,
		Partition:  offset.Partition,
		NextOffset: offset.NextOffset,
	}
	return routeMetadataCommand(ctx, b, raftCommandTxCommitOffset, req, b.applyTxCommitOffset)
}

func (b *Broker) CommitTransaction(ctx context.Context, identity TransactionIdentity) (TransactionBoundaryResult, error) {
	return routeMetadataCommand(ctx, b, raftCommandTxCommit, txBoundaryCommand{Identity: identity}, b.applyTxCommit)
}

func (b *Broker) AbortTransaction(ctx context.Context, identity TransactionIdentity) (TransactionBoundaryResult, error) {
	return routeMetadataCommand(ctx, b, raftCommandTxAbort, txBoundaryCommand{Identity: identity}, b.applyTxAbort)
}

func (b *Broker) selectTransactionPartition(topicName string, partitioning PublishPartitioning, key []byte) (uint32, error) {
	topic, err := b.topicConfig(topicName)
	if err != nil {
		return 0, err
	}
	if topic == nil {
		return 0, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", topicName)
	}
	return b.router.selectPartition(*topic, partitioning, key)
}
