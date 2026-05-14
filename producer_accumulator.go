package ech0

import (
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

type producerAccumulator struct {
	producer *Producer
	batches  *collectionmapping.Map[uint32, *collectionlist.List[producerItem]]
}

func (p *Producer) runAccumulator() {
	acc := newProducerAccumulator(p)
	defer close(p.batch)
	ticker := newProducerTicker(p.opts.linger)
	defer stopProducerTicker(ticker)
	for {
		select {
		case item, ok := <-p.input:
			if !ok {
				acc.flushAll()
				return
			}
			acc.add(item)
		case <-producerTickerC(ticker):
			acc.flushAll()
		case <-p.ctx.Done():
			acc.failAll(producerWrap("producer_context_done", p.ctx.Err(), "run producer accumulator"))
			return
		}
	}
}

func newProducerAccumulator(p *Producer) producerAccumulator {
	return producerAccumulator{
		producer: p,
		batches:  collectionmapping.NewMapWithCapacity[uint32, *collectionlist.List[producerItem]](int(p.partitions)),
	}
}

func (a producerAccumulator) add(item producerItem) {
	items, ok := a.batches.Get(item.partition)
	if !ok {
		items = collectionlist.NewListWithCapacity[producerItem](a.producer.opts.batchSize)
		a.batches.Set(item.partition, items)
	}
	items.Add(item)
	if items.Len() >= a.producer.opts.batchSize {
		a.flushPartition(item.partition)
	}
}

func (a producerAccumulator) flushAll() {
	for _, partition := range a.batches.Keys() {
		a.flushPartition(partition)
	}
}

func (a producerAccumulator) failAll(err error) {
	a.batches.Range(func(_ uint32, items *collectionlist.List[producerItem]) bool {
		items.Range(func(_ int, item producerItem) bool {
			item.future.complete(Message{}, err)
			return true
		})
		return true
	})
}

func (a producerAccumulator) flushPartition(partition uint32) {
	items, ok := a.batches.Get(partition)
	if !ok || items.Len() == 0 {
		return
	}
	a.batches.Delete(partition)
	select {
	case a.producer.batch <- producerBatch{partition: partition, items: items}:
	case <-a.producer.ctx.Done():
		err := producerWrap("producer_context_done", a.producer.ctx.Err(), "flush producer batch")
		items.Range(func(_ int, item producerItem) bool {
			item.future.complete(Message{}, err)
			return true
		})
	}
}

func (p *Producer) runSender() {
	for batch := range p.batch {
		p.publishBatch(batch)
	}
}

func (p *Producer) publishBatch(batch producerBatch) {
	records := make([]store.RecordAppend, 0, batch.items.Len())
	batch.items.Range(func(_ int, item producerItem) bool {
		records = append(records, item.record)
		return true
	})
	partitioning := internalbroker.PublishPartitioning{Mode: internalbroker.PartitionExplicit, Partition: batch.partition}
	idempotency, enabled, err := p.batchIdempotency(batch.partition, len(records))
	if err != nil {
		completeProducerBatchError(batch, err)
		return
	}
	if !enabled {
		idempotency = nil
	}
	result, err := p.publishBatchRecords(partitioning, records, idempotency)
	if err != nil {
		completeProducerBatchError(batch, err)
		return
	}
	p.completeProducerBatch(batch, result)
}

func (p *Producer) publishBatchRecords(
	partitioning internalbroker.PublishPartitioning,
	records []store.RecordAppend,
	idempotency *internalbroker.ProduceIdempotency,
) (internalbroker.ProduceBatchResult, error) {
	if idempotency == nil {
		result, err := p.broker.broker.PublishBatch(p.ctx, p.topic, partitioning, records)
		return result, producerWrap("producer_batch_publish_failed", err, "publish producer batch")
	}
	result, err := p.broker.broker.PublishBatchIdempotent(p.ctx, p.topic, partitioning, records, *idempotency)
	return result, producerWrap("producer_batch_publish_failed", err, "publish idempotent producer batch")
}

func (p *Producer) completeProducerBatch(batch producerBatch, result internalbroker.ProduceBatchResult) {
	if len(result.Records) != batch.items.Len() {
		completeProducerBatchError(batch, producerBatchMismatchError(batch.items.Len(), len(result.Records)))
		return
	}
	batch.items.Range(func(index int, item producerItem) bool {
		item.future.complete(messageFromRecord(p.topic, result.Partition, result.Records[index]), nil)
		return true
	})
}

func completeProducerBatchError(batch producerBatch, err error) {
	batch.items.Range(func(_ int, item producerItem) bool {
		item.future.complete(Message{}, err)
		return true
	})
}

func newProducerTicker(linger time.Duration) *time.Ticker {
	if linger <= 0 {
		return nil
	}
	return time.NewTicker(linger)
}

func stopProducerTicker(ticker *time.Ticker) {
	if ticker != nil {
		ticker.Stop()
	}
}

func producerTickerC(ticker *time.Ticker) <-chan time.Time {
	if ticker == nil {
		return nil
	}
	return ticker.C
}
