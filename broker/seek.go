package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/store"
)

const minTimestampSeekScanBatch = 100

type SeekResult struct {
	Topic       string
	Partition   uint32
	Offset      uint64
	TimestampMS *uint64
}

func (b *Broker) SeekOffset(ctx context.Context, consumer, topic string, partition uint32, offset uint64) (SeekResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, topicResource(identity, topic)); err != nil {
		return SeekResult{}, err
	}
	result, err := b.seekOffsetScoped(ctx, scopedName(identity, "consumer", consumer), scopedTopicName(identity, topic), partition, offset)
	if err != nil {
		return SeekResult{}, err
	}
	return b.visibleSeekResult(identity, result), nil
}

func (b *Broker) SeekTimestamp(ctx context.Context, consumer, topic string, partition uint32, timestampMS uint64) (SeekResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, topicResource(identity, topic)); err != nil {
		return SeekResult{}, err
	}
	scopedTopic := scopedTopicName(identity, topic)
	result, err := b.seekTimestampScoped(ctx, scopedName(identity, "consumer", consumer), scopedTopic, partition, timestampMS)
	if err != nil {
		return SeekResult{}, err
	}
	return b.visibleSeekResult(identity, result), nil
}

func (b *Broker) SeekConsumerGroupOffset(ctx context.Context, group, memberID string, generation uint64, topic string, partition uint32, offset uint64) (SeekResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, groupResource(identity, group)); err != nil {
		return SeekResult{}, err
	}
	scopedGroup := scopedName(identity, "group", group)
	scopedTopic := scopedTopicName(identity, topic)
	if err := b.validateConsumerGroupLease(scopedGroup, memberID, generation, store.NewTopicPartition(scopedTopic, partition)); err != nil {
		return SeekResult{}, err
	}
	result, err := b.seekOffsetScoped(ctx, groupConsumer(scopedGroup), scopedTopic, partition, offset)
	if err != nil {
		return SeekResult{}, err
	}
	return b.visibleSeekResult(identity, result), nil
}

func (b *Broker) SeekConsumerGroupTimestamp(
	ctx context.Context,
	group string,
	memberID string,
	generation uint64,
	topic string,
	partition uint32,
	timestampMS uint64,
) (SeekResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionCommit, groupResource(identity, group)); err != nil {
		return SeekResult{}, err
	}
	scopedGroup := scopedName(identity, "group", group)
	scopedTopic := scopedTopicName(identity, topic)
	if err := b.validateConsumerGroupLease(scopedGroup, memberID, generation, store.NewTopicPartition(scopedTopic, partition)); err != nil {
		return SeekResult{}, err
	}
	result, err := b.seekTimestampScoped(ctx, groupConsumer(scopedGroup), scopedTopic, partition, timestampMS)
	if err != nil {
		return SeekResult{}, err
	}
	return b.visibleSeekResult(identity, result), nil
}

func (b *Broker) seekTimestampScoped(ctx context.Context, consumer, topic string, partition uint32, timestampMS uint64) (SeekResult, error) {
	position, err := b.offsetForTimestamp(topic, partition, timestampMS)
	if err != nil {
		return SeekResult{}, err
	}
	result, err := b.seekOffsetScoped(ctx, consumer, topic, partition, position.Offset)
	result.TimestampMS = position.TimestampMS
	return result, err
}

func (b *Broker) seekOffsetScoped(ctx context.Context, consumer, topic string, partition uint32, offset uint64) (SeekResult, error) {
	endOffset, err := b.seekEndOffset(topic, partition)
	if err != nil {
		return SeekResult{}, err
	}
	if offset > endOffset {
		return SeekResult{}, brokerStoreError(store.CodeInvalidArgument, "seek offset %d is beyond end offset %d for %s/%d", offset, endOffset, topic, partition)
	}
	req := commitOffsetCommand{Consumer: consumer, Topic: topic, Partition: partition, NextOffset: offset}
	if err := b.routeCommitOffset(ctx, req); err != nil {
		return SeekResult{}, err
	}
	return SeekResult{Topic: topic, Partition: partition, Offset: offset}, nil
}

func (b *Broker) offsetForTimestamp(topic string, partition uint32, timestampMS uint64) (SeekResult, error) {
	endOffset, err := b.seekEndOffset(topic, partition)
	if err != nil {
		return SeekResult{}, err
	}
	cursor := uint64(0)
	for cursor < endOffset {
		position, done, err := b.scanTimestampSeekBatch(topic, partition, cursor, timestampMS)
		if err != nil || done {
			return position, err
		}
		cursor = position.Offset
	}
	return SeekResult{Topic: topic, Partition: partition, Offset: endOffset}, nil
}

func (b *Broker) scanTimestampSeekBatch(topic string, partition uint32, offset, timestampMS uint64) (SeekResult, bool, error) {
	records, err := b.queue.ReadFrom(store.NewTopicPartition(topic, partition), offset, b.timestampSeekScanBatch())
	if err != nil {
		return SeekResult{}, false, wrapBrokerStore(err, "read timestamp seek records")
	}
	if len(records) == 0 {
		return SeekResult{Topic: topic, Partition: partition, Offset: offset}, true, nil
	}
	for _, record := range records {
		if record.TimestampMS >= timestampMS {
			matched := record.TimestampMS
			return SeekResult{Topic: topic, Partition: partition, Offset: record.Offset, TimestampMS: &matched}, true, nil
		}
		offset = record.Offset + 1
	}
	return SeekResult{Topic: topic, Partition: partition, Offset: offset}, false, nil
}

func (b *Broker) seekEndOffset(topic string, partition uint32) (uint64, error) {
	if err := b.validateSeekTopicPartition(topic, partition); err != nil {
		return 0, err
	}
	lastOffset, err := b.queue.LastOffset(store.NewTopicPartition(topic, partition))
	if err != nil {
		return 0, wrapBrokerStore(err, "load seek end offset")
	}
	if lastOffset == nil {
		return 0, nil
	}
	if *lastOffset == ^uint64(0) {
		return 0, brokerStoreError(store.CodeInvalidArgument, "last offset overflows next offset for %s/%d", topic, partition)
	}
	return *lastOffset + 1, nil
}

func (b *Broker) validateSeekTopicPartition(topic string, partition uint32) error {
	topicConfig, err := b.topicConfig(topic)
	if err != nil {
		return err
	}
	if topicConfig == nil {
		return brokerStoreError(store.CodeTopicNotFound, "topic %s not found", topic)
	}
	if partition >= topicConfig.Partitions {
		return brokerStoreError(store.CodePartitionNotFound, "partition %s/%d not found", topic, partition)
	}
	return nil
}

func (b *Broker) timestampSeekScanBatch() int {
	if b.cfg.Broker.MaxFetchRecords > minTimestampSeekScanBatch {
		return b.cfg.Broker.MaxFetchRecords
	}
	return minTimestampSeekScanBatch
}

func (b *Broker) visibleSeekResult(identity Identity, result SeekResult) SeekResult {
	result.Topic = visibleTopicName(identity, result.Topic)
	return result
}
