package broker

import (
	"slices"
	"strconv"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) ensureNoOpenTransaction(transactionalID string) error {
	transactions, err := b.meta.ListTransactions()
	if err != nil {
		return wrapBrokerStore(err, "list transactions")
	}
	for index := range transactions {
		if transactions[index].TransactionalID == transactionalID && transactions[index].Status == store.TransactionStatusOpen {
			return brokerStoreError(store.CodeInvalidArgument, "transactional_id %s already has an open transaction", transactionalID)
		}
	}
	return nil
}

func (b *Broker) loadWritableTransaction(
	identity TransactionIdentity,
	topic string,
	partition uint32,
	sequence uint64,
	records int,
) (*store.TransactionState, *store.TransactionPublishedBatch, error) {
	state, err := b.loadOpenTransaction(identity)
	if err != nil {
		return nil, nil, err
	}
	recordCount, err := transactionRecordCount(records)
	if err != nil {
		return nil, nil, err
	}
	if batch, ok, findErr := findPublishedBatch(state, topic, partition, sequence, recordCount); findErr != nil || ok {
		return state, &batch, findErr
	}
	if sequence != state.NextSequence {
		return nil, nil, brokerStoreError(store.CodeInvalidArgument, "transaction sequence %d does not match expected %d", sequence, state.NextSequence)
	}
	return state, nil, nil
}

func (b *Broker) loadOpenTransaction(identity TransactionIdentity) (*store.TransactionState, error) {
	state, err := b.meta.LoadTransaction(identity.TxID)
	if err != nil {
		return nil, wrapBrokerStore(err, "load transaction")
	}
	if state == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "transaction %d not found", identity.TxID)
	}
	if state.ProducerID != identity.ProducerID || state.ProducerEpoch != identity.ProducerEpoch {
		return nil, brokerStoreError(store.CodeInvalidArgument, "transaction identity mismatch")
	}
	if state.Status != store.TransactionStatusOpen {
		return nil, brokerStoreError(store.CodeInvalidArgument, "transaction %d is %s", state.TxID, state.Status)
	}
	if state.ExpiresAtMS > 0 && store.NowMS() >= state.ExpiresAtMS {
		state.Status = store.TransactionStatusAborted
		state.UpdatedAtMS = store.NowMS()
		if err := b.meta.SaveTransaction(*state); err != nil {
			return nil, wrapBrokerStore(err, "expire transaction")
		}
		return nil, brokerStoreError(store.CodeUnavailable, "transaction %d expired", state.TxID)
	}
	return state, nil
}

func (b *Broker) advanceTransactionAfterPublish(state *store.TransactionState, batch store.TransactionPublishedBatch) error {
	nextSequence, err := sequenceAfterCount(batch.BaseSequence, batch.RecordCount)
	if err != nil {
		return err
	}
	state.NextSequence = nextSequence
	state.Partitions = appendTransactionPartition(state.Partitions, store.NewTopicPartition(batch.Topic, batch.Partition))
	state.PublishedBatches = append(state.PublishedBatches, batch)
	state.UpdatedAtMS = store.NowMS()
	if err := b.meta.SaveTransaction(*state); err != nil {
		return wrapBrokerStore(err, "save transaction")
	}
	return nil
}

func appendTransactionPartition(partitions []store.TopicPartition, tp store.TopicPartition) []store.TopicPartition {
	if slices.Contains(partitions, tp) {
		return partitions
	}
	return append(partitions, tp)
}

func transactionIdentityFromState(state store.TransactionState) TransactionIdentity {
	return TransactionIdentity{
		TxID:          state.TxID,
		ProducerID:    state.ProducerID,
		ProducerEpoch: state.ProducerEpoch,
	}
}

func transactionRecordMetadata(identity TransactionIdentity, sequence uint64, controlType store.TransactionControlType) *store.TransactionRecordMetadata {
	return &store.TransactionRecordMetadata{
		TxID:          identity.TxID,
		ProducerID:    identity.ProducerID,
		ProducerEpoch: identity.ProducerEpoch,
		Sequence:      sequence,
		ControlType:   controlType,
	}
}

func sequenceForBatchRecord(base uint64, index int) (uint64, error) {
	offset, err := uint64FromNonNegativeInt(index, "transaction batch record index")
	if err != nil {
		return 0, err
	}
	if base > ^uint64(0)-offset {
		return 0, brokerStoreError(store.CodeInvalidArgument, "transaction sequence overflows uint64")
	}
	return base + offset, nil
}

func uint64FromNonNegativeInt(value int, label string) (uint64, error) {
	if value < 0 {
		return 0, brokerStoreError(store.CodeInvalidArgument, "%s must be non-negative", label)
	}
	out, err := strconv.ParseUint(strconv.Itoa(value), 10, 64)
	if err != nil {
		return 0, wrapBroker("int_to_uint64_failed", err, "convert %s", label)
	}
	return out, nil
}

func transactionPublishBatchResult(txID uint64, partition uint32, records []store.Record) (TransactionPublishBatchResult, error) {
	if len(records) == 0 {
		return TransactionPublishBatchResult{}, brokerStoreError(store.CodeCodec, "transactional publish batch returned no records")
	}
	last := records[len(records)-1]
	return TransactionPublishBatchResult{
		TxID:       txID,
		Partition:  partition,
		Records:    records,
		BaseOffset: records[0].Offset,
		LastOffset: last.Offset,
		NextOffset: last.Offset + 1,
	}, nil
}

func compactTransactionOffsetCommits(offsets []store.TransactionOffsetCommit) []store.TransactionOffsetCommit {
	compacted := collectionmapping.NewMap[commitOffsetApplyKey, store.TransactionOffsetCommit]()
	for _, offset := range offsets {
		key := commitOffsetApplyKey{consumer: transactionOffsetConsumer(offset), topic: offset.Topic, partition: offset.Partition}
		existing, ok := compacted.Get(key)
		if !ok || offset.NextOffset > existing.NextOffset {
			compacted.Set(key, offset)
		}
	}
	return compacted.Values()
}

func transactionOffsetConsumer(offset store.TransactionOffsetCommit) string {
	if offset.Group != "" {
		return groupConsumer(offset.Group)
	}
	return offset.Consumer
}

func transactionOffsetCommitFromStore(offset store.TransactionOffsetCommit) TransactionOffsetCommit {
	return TransactionOffsetCommit{
		Consumer:   offset.Consumer,
		Group:      offset.Group,
		MemberID:   offset.MemberID,
		Generation: offset.Generation,
		Topic:      offset.Topic,
		Partition:  offset.Partition,
		NextOffset: offset.NextOffset,
	}
}
