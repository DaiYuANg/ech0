package broker

import (
	"context"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) BeginTransaction(ctx context.Context, transactionalID string, timeoutMS uint64) (TransactionBeginResult, error) {
	identity := b.identity(ctx)
	transactionalID = strings.TrimSpace(transactionalID)
	if err := b.authorize(ctx, identity, ACLActionTransact, txResource(identity, transactionalID)); err != nil {
		return TransactionBeginResult{}, err
	}
	req := txBeginCommand{TransactionalID: scopedName(identity, "tx", transactionalID), TimeoutMS: timeoutMS}
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
	scope := b.identity(ctx)
	if err := b.authorize(ctx, scope, ACLActionProduce, topicResource(scope, topic)); err != nil {
		return TransactionPublishResult{}, err
	}
	if err := b.checkQuota(ctx, QuotaRequest{Identity: scope, Action: QuotaActionProduce, Topic: topic, Records: 1, Bytes: len(record.Payload)}); err != nil {
		return TransactionPublishResult{}, err
	}
	scopedTopic := scopedTopicName(scope, topic)
	partition, err := b.selectTransactionPartition(scopedTopic, partitioning, record.Key)
	if err != nil {
		return TransactionPublishResult{}, err
	}
	req := txPublishCommand{
		Identity:  identity,
		Sequence:  sequence,
		Topic:     scopedTopic,
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
	scope := b.identity(ctx)
	if err := b.authorize(ctx, scope, ACLActionProduce, topicResource(scope, topic)); err != nil {
		return TransactionPublishBatchResult{}, err
	}
	if err := b.checkQuota(ctx, QuotaRequest{Identity: scope, Action: QuotaActionProduce, Topic: topic, Records: len(records), Bytes: batchPayloadBytes(records)}); err != nil {
		return TransactionPublishBatchResult{}, err
	}
	copied := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for _, record := range records {
		copied.Add(cloneAppend(record))
	}
	scopedTopic := scopedTopicName(scope, topic)
	partition, err := b.selectTransactionPartition(scopedTopic, partitioning, firstRecordKey(copied.Values()))
	if err != nil {
		return TransactionPublishBatchResult{}, err
	}
	req := txPublishBatchCommand{
		Identity:     identity,
		BaseSequence: baseSequence,
		Topic:        scopedTopic,
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
	identityScope := b.identity(ctx)
	if err := b.authorize(ctx, identityScope, ACLActionCommit, topicResource(identityScope, offset.Topic)); err != nil {
		return TransactionOffsetCommitResult{}, err
	}
	req := txCommitOffsetCommand{
		Identity:   identity,
		Consumer:   scopedName(identityScope, "consumer", offset.Consumer),
		Group:      scopedName(identityScope, "group", offset.Group),
		MemberID:   offset.MemberID,
		Generation: offset.Generation,
		Topic:      scopedTopicName(identityScope, offset.Topic),
		Partition:  offset.Partition,
		NextOffset: offset.NextOffset,
		Metadata:   offset.Metadata,
	}
	return routeMetadataCommand(ctx, b, raftCommandTxCommitOffset, req, b.applyTxCommitOffset)
}

func (b *Broker) CommitTransaction(ctx context.Context, identity TransactionIdentity) (TransactionBoundaryResult, error) {
	if err := b.authorize(ctx, b.identity(ctx), ACLActionTransact, ACLResource{Type: ACLResourceTransactionalID}); err != nil {
		return TransactionBoundaryResult{}, err
	}
	return routeMetadataCommand(ctx, b, raftCommandTxCommit, txBoundaryCommand{Identity: identity}, b.applyTxCommit)
}

func (b *Broker) AbortTransaction(ctx context.Context, identity TransactionIdentity) (TransactionBoundaryResult, error) {
	if err := b.authorize(ctx, b.identity(ctx), ACLActionTransact, ACLResource{Type: ACLResourceTransactionalID}); err != nil {
		return TransactionBoundaryResult{}, err
	}
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
