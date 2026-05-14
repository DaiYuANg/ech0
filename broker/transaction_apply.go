package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) applyTxBegin(_ context.Context, req txBeginCommand) (TransactionBeginResult, error) {
	if req.TransactionalID == "" {
		return TransactionBeginResult{}, brokerStoreError(store.CodeInvalidArgument, "transactional_id is required")
	}
	if err := b.ensureNoOpenTransaction(req.TransactionalID); err != nil {
		return TransactionBeginResult{}, err
	}
	timeoutMS := req.TimeoutMS
	if timeoutMS == 0 {
		timeoutMS = defaultTransactionTimeoutMS
	}
	txID, err := b.meta.AllocateTransactionID()
	if err != nil {
		return TransactionBeginResult{}, wrapBrokerStore(err, "allocate transaction id")
	}
	now := store.NowMS()
	state := store.TransactionState{
		TxID:            txID,
		TransactionalID: req.TransactionalID,
		ProducerID:      txID,
		ProducerEpoch:   0,
		Status:          store.TransactionStatusOpen,
		TimeoutMS:       timeoutMS,
		CreatedAtMS:     now,
		UpdatedAtMS:     now,
		ExpiresAtMS:     now + timeoutMS,
	}
	if err := b.meta.SaveTransaction(state); err != nil {
		return TransactionBeginResult{}, wrapBrokerStore(err, "save transaction")
	}
	return TransactionBeginResult{
		Identity:    transactionIdentityFromState(state),
		ExpiresAtMS: state.ExpiresAtMS,
		Status:      state.Status,
	}, nil
}

func (b *Broker) applyTxPublish(ctx context.Context, req txPublishCommand) (TransactionPublishResult, error) {
	state, duplicate, err := b.loadWritableTransaction(req.Identity, req.Topic, req.Partition, req.Sequence, 1)
	if err != nil {
		return TransactionPublishResult{}, err
	}
	if duplicate != nil {
		records, readErr := b.readPublishedBatch(req.Identity, *duplicate)
		if readErr != nil {
			return TransactionPublishResult{}, readErr
		}
		return transactionPublishResult(req.Identity.TxID, req.Partition, records[0]), nil
	}
	record := cloneAppend(req.Record)
	record.Transaction = transactionRecordMetadata(req.Identity, req.Sequence, store.TransactionControlNone)
	appended, err := b.queue.PublishRecord(req.Topic, req.Partition, record)
	if err != nil {
		return TransactionPublishResult{}, wrapBroker("tx_publish_failed", err, "publish transactional record")
	}
	batch, err := newPublishedBatch(req.Topic, req.Partition, req.Sequence, []store.Record{appended})
	if err != nil {
		return TransactionPublishResult{}, err
	}
	if err := b.advanceTransactionAfterPublish(state, batch); err != nil {
		return TransactionPublishResult{}, err
	}
	b.metrics.RecordProduce(ctx, PartitionExplicit, 1)
	b.publishEvent(ctx, RecordProducedEvent{Topic: req.Topic, Partition: req.Partition, Offset: appended.Offset, NextOffset: appended.Offset + 1})
	return transactionPublishResult(req.Identity.TxID, req.Partition, appended), nil
}

func (b *Broker) applyTxPublishBatch(ctx context.Context, req txPublishBatchCommand) (TransactionPublishBatchResult, error) {
	if len(req.Records) == 0 {
		return TransactionPublishBatchResult{}, brokerStoreError(store.CodeInvalidArgument, "transactional batch requires at least one record")
	}
	if validateErr := b.validateBatchPayload(req.Records); validateErr != nil {
		return TransactionPublishBatchResult{}, validateErr
	}
	state, duplicate, err := b.loadWritableTransaction(req.Identity, req.Topic, req.Partition, req.BaseSequence, len(req.Records))
	if err != nil {
		return TransactionPublishBatchResult{}, err
	}
	if duplicate != nil {
		records, readErr := b.readPublishedBatch(req.Identity, *duplicate)
		if readErr != nil {
			return TransactionPublishBatchResult{}, readErr
		}
		return transactionPublishBatchResult(req.Identity.TxID, req.Partition, records)
	}
	records, err := transactionalRecordBatch(req.Identity, req.BaseSequence, req.Records)
	if err != nil {
		return TransactionPublishBatchResult{}, err
	}
	appended, err := b.queue.PublishBatchRecords(req.Topic, req.Partition, records)
	if err != nil {
		return TransactionPublishBatchResult{}, wrapBroker("tx_publish_batch_failed", err, "publish transactional record batch")
	}
	batch, err := newPublishedBatch(req.Topic, req.Partition, req.BaseSequence, appended)
	if err != nil {
		return TransactionPublishBatchResult{}, err
	}
	if err := b.advanceTransactionAfterPublish(state, batch); err != nil {
		return TransactionPublishBatchResult{}, err
	}
	b.recordBatchProduce(ctx, produceBatchCommand{Topic: req.Topic, Partitioning: PublishPartitioning{Mode: PartitionExplicit, Partition: req.Partition}, Records: records}, req.Partition, appended)
	return transactionPublishBatchResult(req.Identity.TxID, req.Partition, appended)
}

func (b *Broker) applyTxCommitOffset(_ context.Context, req txCommitOffsetCommand) (TransactionOffsetCommitResult, error) {
	state, err := b.loadOpenTransaction(req.Identity)
	if err != nil {
		return TransactionOffsetCommitResult{}, err
	}
	offset := store.TransactionOffsetCommit{
		Consumer:   req.Consumer,
		Group:      req.Group,
		MemberID:   req.MemberID,
		Generation: req.Generation,
		Topic:      req.Topic,
		Partition:  req.Partition,
		NextOffset: req.NextOffset,
	}
	if transactionOffsetConsumer(offset) == "" {
		return TransactionOffsetCommitResult{}, brokerStoreError(store.CodeInvalidArgument, "transactional offset commit requires consumer or group")
	}
	state.OffsetCommits = compactTransactionOffsetCommits(append(state.OffsetCommits, offset))
	state.UpdatedAtMS = store.NowMS()
	if err := b.meta.SaveTransaction(*state); err != nil {
		return TransactionOffsetCommitResult{}, wrapBrokerStore(err, "save transaction offset commit")
	}
	return TransactionOffsetCommitResult{TxID: req.Identity.TxID, Offset: transactionOffsetCommitFromStore(offset)}, nil
}

func (b *Broker) applyTxCommit(_ context.Context, req txBoundaryCommand) (TransactionBoundaryResult, error) {
	state, err := b.loadOpenTransaction(req.Identity)
	if err != nil {
		return TransactionBoundaryResult{}, err
	}
	if err := b.appendTransactionControlMarkers(state, store.TransactionControlCommit); err != nil {
		return TransactionBoundaryResult{}, err
	}
	for _, offset := range state.OffsetCommits {
		consumer := transactionOffsetConsumer(offset)
		if err := b.queue.Ack(consumer, offset.Topic, offset.Partition, offset.NextOffset); err != nil {
			return TransactionBoundaryResult{}, wrapBroker("tx_commit_offset_failed", err, "commit transactional offset")
		}
	}
	state.Status = store.TransactionStatusCommitted
	state.UpdatedAtMS = store.NowMS()
	if err := b.meta.SaveTransaction(*state); err != nil {
		return TransactionBoundaryResult{}, wrapBrokerStore(err, "commit transaction")
	}
	return TransactionBoundaryResult{TxID: state.TxID, Status: state.Status}, nil
}

func (b *Broker) applyTxAbort(_ context.Context, req txBoundaryCommand) (TransactionBoundaryResult, error) {
	state, err := b.loadOpenTransaction(req.Identity)
	if err != nil {
		return TransactionBoundaryResult{}, err
	}
	if err := b.appendTransactionControlMarkers(state, store.TransactionControlAbort); err != nil {
		return TransactionBoundaryResult{}, err
	}
	state.Status = store.TransactionStatusAborted
	state.UpdatedAtMS = store.NowMS()
	if err := b.meta.SaveTransaction(*state); err != nil {
		return TransactionBoundaryResult{}, wrapBrokerStore(err, "abort transaction")
	}
	return TransactionBoundaryResult{TxID: state.TxID, Status: state.Status}, nil
}

func transactionalRecordBatch(identity TransactionIdentity, baseSequence uint64, records []store.RecordAppend) ([]store.RecordAppend, error) {
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for index, record := range records {
		copied := cloneAppend(record)
		sequence, err := sequenceForBatchRecord(baseSequence, index)
		if err != nil {
			return nil, err
		}
		copied.Transaction = transactionRecordMetadata(identity, sequence, store.TransactionControlNone)
		out.Add(copied)
	}
	return out.Values(), nil
}

func transactionPublishResult(txID uint64, partition uint32, record store.Record) TransactionPublishResult {
	return TransactionPublishResult{
		TxID:       txID,
		Partition:  partition,
		Record:     record,
		NextOffset: record.Offset + 1,
	}
}
