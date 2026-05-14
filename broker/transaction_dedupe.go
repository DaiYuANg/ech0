package broker

import (
	"strconv"

	"github.com/lyonbrown4d/ech0/store"
)

func findPublishedBatch(
	state *store.TransactionState,
	topic string,
	partition uint32,
	baseSequence uint64,
	recordCount uint64,
) (store.TransactionPublishedBatch, bool, error) {
	for _, batch := range state.PublishedBatches {
		overlaps, err := publishedBatchOverlapsSequence(batch, baseSequence)
		if err != nil {
			return store.TransactionPublishedBatch{}, false, err
		}
		if !overlaps {
			continue
		}
		if batch.BaseSequence != baseSequence {
			return store.TransactionPublishedBatch{}, false, brokerStoreError(
				store.CodeInvalidArgument,
				"transaction sequence %d starts inside published batch %d",
				baseSequence,
				batch.BaseSequence,
			)
		}
		if err := validatePublishedBatchMatch(batch, topic, partition, recordCount); err != nil {
			return store.TransactionPublishedBatch{}, false, err
		}
		return batch, true, nil
	}
	return store.TransactionPublishedBatch{}, false, nil
}

func publishedBatchOverlapsSequence(batch store.TransactionPublishedBatch, sequence uint64) (bool, error) {
	next, err := sequenceAfterCount(batch.BaseSequence, batch.RecordCount)
	if err != nil {
		return false, err
	}
	return sequence >= batch.BaseSequence && sequence < next, nil
}

func validatePublishedBatchMatch(
	batch store.TransactionPublishedBatch,
	topic string,
	partition uint32,
	recordCount uint64,
) error {
	if batch.Topic != topic || batch.Partition != partition || batch.RecordCount != recordCount {
		return brokerStoreError(
			store.CodeInvalidArgument,
			"transaction duplicate sequence %d does not match published batch",
			batch.BaseSequence,
		)
	}
	return nil
}

func newPublishedBatch(topic string, partition uint32, baseSequence uint64, records []store.Record) (store.TransactionPublishedBatch, error) {
	if len(records) == 0 {
		return store.TransactionPublishedBatch{}, brokerStoreError(store.CodeCodec, "transactional publish returned no records")
	}
	if err := validateContiguousPublishedOffsets(records); err != nil {
		return store.TransactionPublishedBatch{}, err
	}
	count, err := transactionRecordCount(len(records))
	if err != nil {
		return store.TransactionPublishedBatch{}, err
	}
	last := records[len(records)-1].Offset
	if last == ^uint64(0) {
		return store.TransactionPublishedBatch{}, brokerStoreError(store.CodeInvalidArgument, "transaction offset overflows uint64")
	}
	return store.TransactionPublishedBatch{
		Topic:        topic,
		Partition:    partition,
		BaseSequence: baseSequence,
		RecordCount:  count,
		BaseOffset:   records[0].Offset,
		LastOffset:   last,
		NextOffset:   last + 1,
	}, nil
}

func validateContiguousPublishedOffsets(records []store.Record) error {
	baseOffset := records[0].Offset
	for index, record := range records {
		delta, err := uint64FromNonNegativeInt(index, "published record index")
		if err != nil {
			return err
		}
		if baseOffset > ^uint64(0)-delta {
			return brokerStoreError(store.CodeInvalidArgument, "transaction offset overflows uint64")
		}
		expected := baseOffset + delta
		if record.Offset != expected {
			return brokerStoreError(store.CodeCodec, "transactional publish returned non-contiguous offsets")
		}
	}
	return nil
}

func (b *Broker) readPublishedBatch(
	identity TransactionIdentity,
	batch store.TransactionPublishedBatch,
) ([]store.Record, error) {
	recordCount, err := intFromUint64(batch.RecordCount, "published transaction record count")
	if err != nil {
		return nil, err
	}
	records, err := b.queue.ReadFrom(store.NewTopicPartition(batch.Topic, batch.Partition), batch.BaseOffset, recordCount)
	if err != nil {
		return nil, wrapBrokerStore(err, "read published transaction batch")
	}
	if err := validatePublishedBatchRecords(identity, batch, records); err != nil {
		return nil, err
	}
	return records, nil
}

func validatePublishedBatchRecords(
	identity TransactionIdentity,
	batch store.TransactionPublishedBatch,
	records []store.Record,
) error {
	actualCount, err := uint64FromNonNegativeInt(len(records), "published transaction record count")
	if err != nil {
		return err
	}
	if actualCount != batch.RecordCount {
		return brokerStoreError(store.CodeUnavailable, "published transaction batch is not fully readable")
	}
	if records[0].Offset != batch.BaseOffset || records[len(records)-1].Offset != batch.LastOffset {
		return brokerStoreError(store.CodeUnavailable, "published transaction batch offsets changed")
	}
	return validatePublishedBatchRecordMetadata(identity, batch, records)
}

func validatePublishedBatchRecordMetadata(
	identity TransactionIdentity,
	batch store.TransactionPublishedBatch,
	records []store.Record,
) error {
	for index, record := range records {
		if record.Transaction == nil {
			return brokerStoreError(store.CodeUnavailable, "published transaction batch metadata is missing")
		}
		sequence, err := sequenceForBatchRecord(batch.BaseSequence, index)
		if err != nil {
			return err
		}
		if !transactionMetadataMatchesIdentity(*record.Transaction, identity, sequence) {
			return brokerStoreError(store.CodeUnavailable, "published transaction batch metadata changed")
		}
	}
	return nil
}

func transactionMetadataMatchesIdentity(
	metadata store.TransactionRecordMetadata,
	identity TransactionIdentity,
	sequence uint64,
) bool {
	return metadata.TxID == identity.TxID &&
		metadata.ProducerID == identity.ProducerID &&
		metadata.ProducerEpoch == identity.ProducerEpoch &&
		metadata.Sequence == sequence &&
		metadata.ControlType == store.TransactionControlNone
}

func transactionRecordCount(records int) (uint64, error) {
	if records <= 0 {
		return 0, brokerStoreError(store.CodeInvalidArgument, "transactional publish requires at least one record")
	}
	return uint64FromNonNegativeInt(records, "transaction record count")
}

func sequenceAfterCount(base, count uint64) (uint64, error) {
	if base > ^uint64(0)-count {
		return 0, brokerStoreError(store.CodeInvalidArgument, "transaction sequence overflows uint64")
	}
	return base + count, nil
}

func intFromUint64(value uint64, label string) (int, error) {
	out, err := strconv.Atoi(strconv.FormatUint(value, 10))
	if err != nil {
		return 0, wrapBroker("uint64_to_int_failed", err, "convert %s", label)
	}
	return out, nil
}
