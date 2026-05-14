package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) applyProduceBatches(ctx context.Context, req produceBatchesCommand) (produceBatchesResult, error) {
	items := make([]produceBatchItemResult, len(req.Requests))
	plans := collectionlist.NewListWithCapacity[produceBatchPlan](len(req.Requests))
	for index, batch := range req.Requests {
		plan, err := b.planProduceBatch(index, batch)
		if err != nil {
			items[index].Error = err.Error()
			continue
		}
		plans.Add(plan)
	}
	groups := groupProduceBatchPlans(plans.Values())
	groups.Range(func(_ store.TopicPartition, produceGroup produceBatchGroup) bool {
		b.applyProduceBatchGroup(ctx, req.Requests, produceGroup, items)
		return true
	})
	b.recordProduceBatchItems(ctx, req.Requests, items)
	return produceBatchesResult{Items: items}, nil
}

type produceBatchPlan struct {
	index int
	tp    store.TopicPartition
}

type produceBatchGroup struct {
	tp      store.TopicPartition
	indexes []int
}

func (b *Broker) planProduceBatch(index int, req produceBatchCommand) (produceBatchPlan, error) {
	topic, err := b.topicConfig(req.Topic)
	if err != nil {
		return produceBatchPlan{}, err
	}
	if topic == nil {
		return produceBatchPlan{}, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", req.Topic)
	}
	if validateErr := b.validateBatchPayload(req.Records); validateErr != nil {
		return produceBatchPlan{}, validateErr
	}
	partition, err := b.router.selectPartition(*topic, req.Partitioning, firstRecordKey(req.Records))
	if err != nil {
		return produceBatchPlan{}, err
	}
	return produceBatchPlan{
		index: index,
		tp:    store.NewTopicPartition(req.Topic, partition),
	}, nil
}

func groupProduceBatchPlans(plans []produceBatchPlan) *collectionmapping.Map[store.TopicPartition, produceBatchGroup] {
	groups := collectionmapping.NewMapWithCapacity[store.TopicPartition, produceBatchGroup](len(plans))
	for _, plan := range plans {
		group := groups.GetOrDefault(plan.tp, produceBatchGroup{tp: plan.tp})
		group.indexes = append(group.indexes, plan.index)
		groups.Set(plan.tp, group)
	}
	return groups
}

func (b *Broker) applyProduceBatchGroup(ctx context.Context, requests []produceBatchCommand, group produceBatchGroup, items []produceBatchItemResult) {
	if err := ctx.Err(); err != nil {
		setProduceBatchGroupError(items, group.indexes, err)
		return
	}
	records, err := b.queue.PublishBatchRecords(group.tp.Topic, group.tp.Partition, groupRecords(requests, group.indexes))
	if err != nil {
		setProduceBatchGroupError(items, group.indexes, wrapBroker("publish_batch_failed", err, "publish record batch"))
		return
	}
	if splitErr := splitProduceBatchGroupRecords(requests, group, records, items); splitErr != nil {
		setProduceBatchGroupError(items, group.indexes, splitErr)
	}
}

func groupRecords(requests []produceBatchCommand, indexes []int) []store.RecordAppend {
	total := 0
	for _, index := range indexes {
		total += len(requests[index].Records)
	}
	records := collectionlist.NewListWithCapacity[store.RecordAppend](total)
	for _, index := range indexes {
		records.Add(requests[index].Records...)
	}
	return records.Values()
}

func splitProduceBatchGroupRecords(
	requests []produceBatchCommand,
	group produceBatchGroup,
	records []store.Record,
	items []produceBatchItemResult,
) error {
	cursor := 0
	for _, index := range group.indexes {
		count := len(requests[index].Records)
		if cursor+count > len(records) {
			return brokerStoreError(store.CodeCodec, "produce batch group result length mismatch")
		}
		items[index] = produceBatchItemResult{
			Result: ProduceBatchResult{
				Partition: group.tp.Partition,
				Records:   records[cursor : cursor+count],
			},
		}
		cursor += count
	}
	if cursor != len(records) {
		return brokerStoreError(store.CodeCodec, "produce batch group returned %d unassigned records", len(records)-cursor)
	}
	return nil
}

func setProduceBatchGroupError(items []produceBatchItemResult, indexes []int, err error) {
	message := errorMessage(err)
	for _, index := range indexes {
		items[index].Error = message
	}
}

func (b *Broker) recordProduceBatchItems(ctx context.Context, requests []produceBatchCommand, items []produceBatchItemResult) {
	for index, item := range items {
		if item.Error == "" {
			b.recordBatchProduce(ctx, requests[index], item.Result.Partition, item.Result.Records)
		}
	}
}
