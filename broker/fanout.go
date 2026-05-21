package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

type FanoutResult struct {
	Topic   string
	Records []FanoutRecordResult
}

type FanoutRecordResult struct {
	Partition  uint32
	Record     store.Record
	NextOffset uint64
	Error      string
}

func (b *Broker) PublishFanout(ctx context.Context, topic string, key []byte, tombstone bool, payload []byte) (FanoutResult, error) {
	record := store.NewRecordAppend(payload)
	record.Key = append([]byte(nil), key...)
	if tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	return b.PublishFanoutRecord(ctx, topic, record)
}

func (b *Broker) PublishFanoutRecord(ctx context.Context, topic string, record store.RecordAppend) (FanoutResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionProduce, topicResource(identity, topic)); err != nil {
		return FanoutResult{}, err
	}
	scopedTopic := scopedTopicName(identity, topic)
	topicConfig, err := b.loadTopicConfig(scopedTopic)
	if err != nil {
		return FanoutResult{}, err
	}
	if quotaErr := b.checkFanoutQuota(ctx, identity, topic, topicConfig.Partitions, record); quotaErr != nil {
		return FanoutResult{}, quotaErr
	}
	result, err := b.publishFanoutScoped(ctx, scopedTopic, record, topicConfig.Partitions)
	result.Topic = visibleTopicName(identity, result.Topic)
	return result, err
}

func (b *Broker) publishFanoutScoped(ctx context.Context, topic string, record store.RecordAppend, partitions uint32) (FanoutResult, error) {
	partitionCount, err := intFromUint64(uint64(partitions), "fanout partition count")
	if err != nil {
		return FanoutResult{}, err
	}
	requests := collectionlist.NewListWithCapacity[produceBatchCommand](partitionCount)
	for partition := range partitions {
		requests.Add(produceBatchCommand{
			Topic: topic,
			Partitioning: PublishPartitioning{
				Mode:      PartitionExplicit,
				Partition: partition,
			},
			Records: []store.RecordAppend{cloneAppend(record)},
		})
	}
	req := produceBatchesCommand{Requests: requests.Values()}
	var result produceBatchesResult
	if b.usesClusterCommandRouter() {
		result, err = b.routeProduceBatchesCommand(ctx, req)
	} else {
		result, err = b.applyProduceBatches(ctx, req)
	}
	if err != nil {
		return FanoutResult{}, err
	}
	return fanoutResultFromProduceBatches(topic, result)
}

func fanoutResultFromProduceBatches(topic string, result produceBatchesResult) (FanoutResult, error) {
	records := collectionlist.NewListWithCapacity[FanoutRecordResult](len(result.Items))
	failures := 0
	for index := range result.Items {
		item := &result.Items[index]
		record := FanoutRecordResult{Error: item.Error}
		if item.Error != "" {
			failures++
			records.Add(record)
			continue
		}
		if len(item.Result.Records) == 0 {
			record.Error = "fanout append returned no records"
			failures++
			records.Add(record)
			continue
		}
		appended := item.Result.Records[0]
		record.Partition = item.Result.Partition
		record.Record = appended
		record.NextOffset = appended.Offset + 1
		records.Add(record)
	}
	out := FanoutResult{Topic: topic, Records: records.Values()}
	if failures > 0 {
		return out, brokerStoreError(store.CodeUnavailable, "fanout publish failed for %d partitions", failures)
	}
	return out, nil
}

func (b *Broker) checkFanoutQuota(ctx context.Context, identity Identity, topic string, partitions uint32, record store.RecordAppend) error {
	records, payloadBytes, storageBytes, err := fanoutQuotaUsage(partitions, record)
	if err != nil {
		return err
	}
	return b.checkProduceQuota(ctx, identity, topic, records, payloadBytes, storageBytes)
}

func fanoutQuotaUsage(partitions uint32, record store.RecordAppend) (int, int, uint64, error) {
	if partitions == 0 {
		return 0, 0, 0, brokerStoreError(store.CodeInvalidArgument, "fanout requires at least one partition")
	}
	records, err := intFromUint64(uint64(partitions), "fanout partition count")
	if err != nil {
		return 0, 0, 0, err
	}
	payloadBytes, err := checkedIntProduct(records, len(record.Payload), "fanout payload bytes")
	if err != nil {
		return 0, 0, 0, err
	}
	storageBytes, err := checkedUint64Product(uint64(partitions), store.RecordAppendStorageBytes(record), "fanout storage bytes")
	if err != nil {
		return 0, 0, 0, err
	}
	return records, payloadBytes, storageBytes, nil
}

func checkedIntProduct(left, right int, label string) (int, error) {
	if left < 0 || right < 0 {
		return 0, brokerStoreError(store.CodeInvalidArgument, "%s must be non-negative", label)
	}
	maxInt := int(^uint(0) >> 1)
	if left != 0 && right > maxInt/left {
		return 0, brokerStoreError(store.CodeInvalidArgument, "%s overflows int", label)
	}
	return left * right, nil
}

func checkedUint64Product(left, right uint64, label string) (uint64, error) {
	if left != 0 && right > ^uint64(0)/left {
		return 0, brokerStoreError(store.CodeInvalidArgument, "%s overflows uint64", label)
	}
	return left * right, nil
}
