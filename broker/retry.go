package broker

import (
	"context"
	"fmt"

	"github.com/DaiYuANg/ech0/store"
)

func (b *Broker) Nack(ctx context.Context, consumer, topic string, partition uint32, offset uint64, lastError *string) (RetryResult, error) {
	req := nackCommand{Consumer: consumer, Topic: topic, Partition: partition, Offset: offset, LastError: lastError}
	return proposeOrApply(ctx, b, raftCommandNack, req, b.applyNack)
}

func (b *Broker) ProcessRetryBatch(ctx context.Context, consumer, sourceTopic string, partition uint32, maxRecords int) (ProcessRetryResult, error) {
	req := processRetryCommand{Consumer: consumer, SourceTopic: sourceTopic, Partition: partition, MaxRecords: maxRecords, NowMS: store.NowMS()}
	return proposeOrApply(ctx, b, raftCommandProcessRetry, req, b.applyProcessRetryBatch)
}

func (b *Broker) ProcessRetryTopicsOnce(ctx context.Context, consumerPrefix string, maxRecordsPerPartition int) (int, error) {
	consumerPrefix, maxRecordsPerPartition = b.retryWorkerConfig(consumerPrefix, maxRecordsPerPartition)
	topics, err := b.meta.ListTopics()
	if err != nil {
		return 0, wrapBrokerStore(err, "list topics for retry worker")
	}
	return b.processRetryTopics(ctx, topics, consumerPrefix, maxRecordsPerPartition)
}

func (b *Broker) retryWorkerConfig(consumerPrefix string, maxRecordsPerPartition int) (string, int) {
	if consumerPrefix == "" {
		consumerPrefix = b.cfg.Broker.RetryWorkerConsumerPrefix
	}
	if maxRecordsPerPartition <= 0 {
		maxRecordsPerPartition = b.cfg.Broker.RetryWorkerMaxRecords
	}
	return consumerPrefix, maxRecordsPerPartition
}

func (b *Broker) processRetryTopics(
	ctx context.Context,
	topics []store.TopicConfig,
	consumerPrefix string,
	maxRecordsPerPartition int,
) (int, error) {
	movedTotal := 0
	for i := range topics {
		moved, err := b.processRetryTopic(ctx, topics[i], consumerPrefix, maxRecordsPerPartition)
		if err != nil {
			return movedTotal, err
		}
		movedTotal += moved
	}
	return movedTotal, nil
}

func (b *Broker) processRetryTopic(ctx context.Context, topic store.TopicConfig, consumerPrefix string, maxRecords int) (int, error) {
	if isInternalTopicName(topic.Name) {
		return 0, nil
	}
	exists, err := b.log.TopicExists(retryTopicName(topic.Name))
	if err != nil {
		return 0, wrapBrokerStore(err, "check retry topic")
	}
	if !exists {
		return 0, nil
	}
	return b.processRetryTopicPartitions(ctx, topic, consumerPrefix, maxRecords)
}

func (b *Broker) processRetryTopicPartitions(ctx context.Context, topic store.TopicConfig, consumerPrefix string, maxRecords int) (int, error) {
	movedTotal := 0
	for partition := range topic.Partitions {
		consumer := fmt.Sprintf("%s:%s:%d", consumerPrefix, topic.Name, partition)
		result, err := b.ProcessRetryBatch(ctx, consumer, topic.Name, partition, maxRecords)
		if err != nil {
			return movedTotal, err
		}
		movedTotal += result.MovedToOrigin + result.MovedToDeadLetter
	}
	return movedTotal, nil
}

type nackCommand struct {
	Consumer  string
	Topic     string
	Partition uint32
	Offset    uint64
	LastError *string
}

type processRetryCommand struct {
	Consumer    string
	SourceTopic string
	Partition   uint32
	MaxRecords  int
	NowMS       uint64
}
