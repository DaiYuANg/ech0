package broker

import (
	"cmp"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

const producerDedupeWindowBatches = 1024

type appendProduceBatchResult struct {
	Records  []store.Record
	Appended bool
}

func (b *Broker) appendProduceBatch(topic string, partition uint32, req produceBatchCommand) (appendProduceBatchResult, error) {
	if req.Idempotency == nil {
		records, err := b.queue.PublishBatchRecords(topic, partition, req.Records)
		if err != nil {
			return appendProduceBatchResult{}, wrapBroker("publish_batch_failed", err, "publish record batch")
		}
		return appendProduceBatchResult{Records: records, Appended: true}, nil
	}
	b.producerDedupeMu.Lock()
	defer b.producerDedupeMu.Unlock()
	return b.appendIdempotentProduceBatch(topic, partition, req)
}

func (b *Broker) appendIdempotentProduceBatch(topic string, partition uint32, req produceBatchCommand) (appendProduceBatchResult, error) {
	id := req.Idempotency
	recordCount, err := producerRecordCount(len(req.Records))
	if err != nil {
		return appendProduceBatchResult{}, err
	}
	batches, filter, err := b.loadProducerDedupeBatches(topic, partition, *id, recordCount)
	if err != nil {
		return appendProduceBatchResult{}, err
	}
	if batch, ok, findErr := findProducerPublishedBatch(batches, *id, recordCount); findErr != nil || ok {
		if findErr != nil {
			return appendProduceBatchResult{}, findErr
		}
		records, readErr := b.readProducerPublishedBatch(batch)
		if readErr != nil {
			return appendProduceBatchResult{}, readErr
		}
		return appendProduceBatchResult{Records: records}, nil
	}
	records, err := b.queue.PublishBatchRecords(topic, partition, req.Records)
	if err != nil {
		return appendProduceBatchResult{}, wrapBroker("publish_batch_failed", err, "publish record batch")
	}
	if err := b.saveProducerDedupeBatch(filter, *id, topic, partition, records); err != nil {
		return appendProduceBatchResult{}, err
	}
	return appendProduceBatchResult{Records: records, Appended: true}, nil
}

func (b *Broker) loadProducerDedupeBatches(
	topic string,
	partition uint32,
	id ProduceIdempotency,
	recordCount uint64,
) ([]store.ProducerPublishedBatch, store.ProducerBatchFilter, error) {
	validateErr := validateProduceIdempotency(id, recordCount)
	if validateErr != nil {
		return nil, store.ProducerBatchFilter{}, validateErr
	}
	filter := store.ProducerBatchFilter{
		ProducerID:   id.ProducerID,
		Topic:        topic,
		Partition:    partition,
		PartitionSet: true,
	}
	batches, err := b.meta.ListProducerBatches(filter)
	if err != nil {
		return nil, store.ProducerBatchFilter{}, wrapBrokerStore(err, "list producer batches")
	}
	if epochErr := ensureProducerEpochWritable(id, batches); epochErr != nil {
		return nil, store.ProducerBatchFilter{}, epochErr
	}
	return batches, filter, nil
}

func (b *Broker) saveProducerDedupeBatch(
	filter store.ProducerBatchFilter,
	id ProduceIdempotency,
	topic string,
	partition uint32,
	records []store.Record,
) error {
	batch, err := newProducerPublishedBatch(id, topic, partition, records)
	if err != nil {
		return err
	}
	saveErr := b.meta.SaveProducerBatch(batch)
	if saveErr != nil {
		return wrapBrokerStore(saveErr, "save producer batch")
	}
	return b.pruneProducerDedupeWindow(filter)
}

func validateProduceIdempotency(id ProduceIdempotency, recordCount uint64) error {
	if id.ProducerID == 0 {
		return brokerStoreError(store.CodeInvalidArgument, "producer_id is required for idempotent publish")
	}
	_, err := sequenceAfterCount(id.BaseSequence, recordCount)
	return err
}

func producerRecordCount(records int) (uint64, error) {
	if records <= 0 {
		return 0, brokerStoreError(store.CodeInvalidArgument, "idempotent publish requires at least one record")
	}
	return uint64FromNonNegativeInt(records, "producer record count")
}

func ensureProducerEpochWritable(id ProduceIdempotency, batches []store.ProducerPublishedBatch) error {
	for _, batch := range batches {
		if batch.ProducerEpoch > id.ProducerEpoch {
			return brokerStoreError(store.CodeInvalidArgument, "producer %d epoch %d is fenced by epoch %d", id.ProducerID, id.ProducerEpoch, batch.ProducerEpoch)
		}
	}
	return nil
}

func findProducerPublishedBatch(
	batches []store.ProducerPublishedBatch,
	id ProduceIdempotency,
	recordCount uint64,
) (store.ProducerPublishedBatch, bool, error) {
	for _, batch := range batches {
		if batch.ProducerEpoch != id.ProducerEpoch {
			continue
		}
		overlaps, err := producerBatchOverlapsSequence(batch, id.BaseSequence)
		if err != nil {
			return store.ProducerPublishedBatch{}, false, err
		}
		if !overlaps {
			continue
		}
		if batch.BaseSequence != id.BaseSequence || batch.RecordCount != recordCount {
			return store.ProducerPublishedBatch{}, false, brokerStoreError(store.CodeInvalidArgument, "producer duplicate sequence %d does not match published batch", id.BaseSequence)
		}
		return batch, true, nil
	}
	return store.ProducerPublishedBatch{}, false, nil
}

func producerBatchOverlapsSequence(batch store.ProducerPublishedBatch, sequence uint64) (bool, error) {
	next, err := sequenceAfterCount(batch.BaseSequence, batch.RecordCount)
	if err != nil {
		return false, err
	}
	return sequence >= batch.BaseSequence && sequence < next, nil
}

func (b *Broker) readProducerPublishedBatch(batch store.ProducerPublishedBatch) ([]store.Record, error) {
	recordCount, err := intFromUint64(batch.RecordCount, "published producer record count")
	if err != nil {
		return nil, err
	}
	records, err := b.queue.ReadFrom(store.NewTopicPartition(batch.Topic, batch.Partition), batch.BaseOffset, recordCount)
	if err != nil {
		return nil, wrapBrokerStore(err, "read published producer batch")
	}
	if err := validateProducerPublishedRecords(batch, records); err != nil {
		return nil, err
	}
	return records, nil
}

func validateProducerPublishedRecords(batch store.ProducerPublishedBatch, records []store.Record) error {
	actualCount, err := uint64FromNonNegativeInt(len(records), "published producer record count")
	if err != nil {
		return err
	}
	if actualCount != batch.RecordCount {
		return brokerStoreError(store.CodeUnavailable, "published producer batch is not fully readable")
	}
	if records[0].Offset != batch.BaseOffset || records[len(records)-1].Offset != batch.LastOffset {
		return brokerStoreError(store.CodeUnavailable, "published producer batch offsets changed")
	}
	return nil
}

func newProducerPublishedBatch(
	id ProduceIdempotency,
	topic string,
	partition uint32,
	records []store.Record,
) (store.ProducerPublishedBatch, error) {
	if len(records) == 0 {
		return store.ProducerPublishedBatch{}, brokerStoreError(store.CodeCodec, "idempotent publish returned no records")
	}
	if err := validateProducerPublishedOffsets(records); err != nil {
		return store.ProducerPublishedBatch{}, err
	}
	count, err := producerRecordCount(len(records))
	if err != nil {
		return store.ProducerPublishedBatch{}, err
	}
	last := records[len(records)-1].Offset
	if last == ^uint64(0) {
		return store.ProducerPublishedBatch{}, brokerStoreError(store.CodeInvalidArgument, "producer offset overflows uint64")
	}
	return store.ProducerPublishedBatch{
		ProducerID:    id.ProducerID,
		ProducerEpoch: id.ProducerEpoch,
		Topic:         topic,
		Partition:     partition,
		BaseSequence:  id.BaseSequence,
		RecordCount:   count,
		BaseOffset:    records[0].Offset,
		LastOffset:    last,
		NextOffset:    last + 1,
		UpdatedAtMS:   store.NowMS(),
	}, nil
}

func validateProducerPublishedOffsets(records []store.Record) error {
	baseOffset := records[0].Offset
	for index, record := range records {
		delta, err := uint64FromNonNegativeInt(index, "producer batch record index")
		if err != nil {
			return err
		}
		if baseOffset > ^uint64(0)-delta {
			return brokerStoreError(store.CodeInvalidArgument, "producer offset overflows uint64")
		}
		if record.Offset != baseOffset+delta {
			return brokerStoreError(store.CodeCodec, "idempotent publish returned non-contiguous offsets")
		}
	}
	return nil
}

func (b *Broker) pruneProducerDedupeWindow(filter store.ProducerBatchFilter) error {
	batches, err := b.meta.ListProducerBatches(filter)
	if err != nil {
		return wrapBrokerStore(err, "list producer batches for pruning")
	}
	if len(batches) <= producerDedupeWindowBatches {
		return nil
	}
	ordered := collectionlist.NewList(batches...).
		Sort(func(left, right store.ProducerPublishedBatch) int {
			if left.UpdatedAtMS != right.UpdatedAtMS {
				return cmp.Compare(left.UpdatedAtMS, right.UpdatedAtMS)
			}
			if left.ProducerEpoch != right.ProducerEpoch {
				return cmp.Compare(left.ProducerEpoch, right.ProducerEpoch)
			}
			return cmp.Compare(left.BaseSequence, right.BaseSequence)
		}).
		Values()
	removeCount := len(ordered) - producerDedupeWindowBatches
	for _, batch := range ordered[:removeCount] {
		if err := b.meta.DeleteProducerBatch(batch); err != nil {
			return wrapBrokerStore(err, "delete producer batch")
		}
	}
	return nil
}

func cloneProduceIdempotency(id *ProduceIdempotency) *ProduceIdempotency {
	if id == nil {
		return nil
	}
	cp := *id
	return &cp
}
