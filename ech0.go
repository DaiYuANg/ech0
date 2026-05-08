// Package ech0 exposes an embedded library-first message broker API.
package ech0

import (
	"context"
	"errors"
	"log/slog"
	"time"

	internalbroker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
	"github.com/samber/oops"
)

type Broker struct {
	broker    *internalbroker.Broker
	scheduled *internalbroker.ScheduledRuntime
	logStore  *store.StorxLogStore
	metaStore *store.StorxMetadataStore
}

func Open(ctx context.Context, opts Options) (*Broker, error) {
	opts = normalizeOptions(opts)
	cfg := configFromOptions(opts)

	logStore, err := store.OpenStorxLogStore(cfg.SegmentLogPath())
	if err != nil {
		return nil, oops.In("embedded").Code("open_log_store_failed").Wrapf(err, "open log store")
	}
	metaStore, err := store.OpenStorxMetadataStoreWithOptionsContext(ctx, cfg.MetadataPath(), store.StorxMetadataOptions{})
	if err != nil {
		return nil, errors.Join(
			oops.In("embedded").Code("open_metadata_store_failed").Wrapf(err, "open metadata store"),
			closeLogStore(logStore),
		)
	}
	b, err := internalbroker.NewWithStores(cfg, logStore, metaStore)
	if err != nil {
		return nil, errors.Join(
			oops.In("embedded").Code("broker_init_failed").Wrapf(err, "initialize broker"),
			closeMetadataStore(metaStore),
			closeLogStore(logStore),
		)
	}
	if startErr := b.Start(ctx); startErr != nil {
		return nil, errors.Join(
			oops.In("embedded").Code("broker_start_failed").Wrapf(startErr, "start broker"),
			closeMetadataStore(metaStore),
			closeLogStore(logStore),
		)
	}
	scheduled, err := internalbroker.NewScheduledRuntime(cfg, b, slog.Default())
	if err != nil {
		return nil, errors.Join(
			oops.In("embedded").Code("scheduler_init_failed").Wrapf(err, "initialize scheduler"),
			stopInternalBroker(ctx, b),
			closeMetadataStore(metaStore),
			closeLogStore(logStore),
		)
	}
	if err := scheduled.Start(ctx); err != nil {
		return nil, errors.Join(
			oops.In("embedded").Code("scheduler_start_failed").Wrapf(err, "start scheduler"),
			stopInternalBroker(ctx, b),
			closeMetadataStore(metaStore),
			closeLogStore(logStore),
		)
	}
	return &Broker{broker: b, scheduled: scheduled, logStore: logStore, metaStore: metaStore}, nil
}

func Run(ctx context.Context, opts Options) (err error) {
	b, err := Open(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, b.Close(ctx))
	}()
	<-ctx.Done()
	return oops.In("embedded").Code("context_done").Wrap(ctx.Err())
}

func (b *Broker) Close(ctx context.Context) error {
	if b == nil {
		return nil
	}
	var result error
	if b.scheduled != nil {
		result = errors.Join(result, stopScheduler(ctx, b.scheduled))
	}
	if b.broker != nil {
		result = errors.Join(result, stopInternalBroker(ctx, b.broker))
	}
	if b.metaStore != nil {
		result = errors.Join(result, closeMetadataStore(b.metaStore))
	}
	if b.logStore != nil {
		result = errors.Join(result, closeLogStore(b.logStore))
	}
	return result
}

func (b *Broker) CreateTopic(ctx context.Context, name string, opts ...TopicOption) error {
	topicOpts := topicOptions{partitions: 1}
	for _, opt := range opts {
		if opt != nil {
			opt(&topicOpts)
		}
	}
	topic := store.NewTopicConfig(name)
	topic.Partitions = topicOpts.partitions
	topic.DelayEnabled = topicOpts.delayEnabled
	if topicOpts.deadLetterTopic != "" {
		topic.DeadLetterTopic = &topicOpts.deadLetterTopic
	}
	if topicOpts.retryPolicy != nil {
		topic.RetryPolicy = store.TopicRetryPolicy{
			MaxAttempts:      topicOpts.retryPolicy.MaxAttempts,
			BackoffInitialMS: durationMillis(topicOpts.retryPolicy.InitialBackoff),
			BackoffMaxMS:     durationMillis(topicOpts.retryPolicy.MaxBackoff),
		}
	}
	_, err := b.broker.CreateTopic(ctx, topic)
	return oops.In("embedded").Code("create_topic_failed").With("topic", name).Wrapf(err, "create topic")
}

func (b *Broker) Publish(ctx context.Context, topic string, payload []byte, opts ...PublishOption) (Message, error) {
	publishOpts := publishOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&publishOpts)
		}
	}
	partitioning := internalbroker.PublishPartitioning{Mode: internalbroker.PartitionRoundRobin}
	if publishOpts.partition != nil {
		partitioning = internalbroker.PublishPartitioning{Mode: internalbroker.PartitionExplicit, Partition: *publishOpts.partition}
	} else if len(publishOpts.key) > 0 {
		partitioning = internalbroker.PublishPartitioning{Mode: internalbroker.PartitionKeyHash}
	}
	result, err := b.broker.Publish(ctx, topic, partitioning, publishOpts.key, publishOpts.tombstone, payload)
	if err != nil {
		return Message{}, oops.In("embedded").Code("publish_failed").With("topic", topic).Wrapf(err, "publish message")
	}
	return messageFromRecord(topic, result.Partition, result.Record), nil
}

func (b *Broker) PublishBatch(ctx context.Context, topic string, payloads [][]byte, opts ...PublishOption) ([]Message, error) {
	publishOpts := publishOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&publishOpts)
		}
	}
	partitioning := internalbroker.PublishPartitioning{Mode: internalbroker.PartitionRoundRobin}
	if publishOpts.partition != nil {
		partitioning = internalbroker.PublishPartitioning{Mode: internalbroker.PartitionExplicit, Partition: *publishOpts.partition}
	} else if len(publishOpts.key) > 0 {
		partitioning = internalbroker.PublishPartitioning{Mode: internalbroker.PartitionKeyHash}
	}
	records := make([]store.RecordAppend, 0, len(payloads))
	for _, payload := range payloads {
		record := store.NewRecordAppend(payload)
		record.Key = append([]byte(nil), publishOpts.key...)
		if publishOpts.tombstone {
			record.Attributes |= store.RecordAttributeTombstone
		}
		records = append(records, record)
	}
	result, err := b.broker.PublishBatch(ctx, topic, partitioning, records)
	if err != nil {
		return nil, oops.In("embedded").Code("publish_batch_failed").With("topic", topic).Wrapf(err, "publish message batch")
	}
	messages := make([]Message, 0, len(result.Records))
	for _, record := range result.Records {
		messages = append(messages, messageFromRecord(topic, result.Partition, record))
	}
	return messages, nil
}

func (b *Broker) Fetch(ctx context.Context, consumer, topic string, opts ...FetchOption) (FetchResult, error) {
	fetchOpts := fetchOptions{maxRecords: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&fetchOpts)
		}
	}
	poll, err := b.broker.Fetch(ctx, consumer, topic, fetchOpts.partition, fetchOpts.offset, fetchOpts.maxRecords)
	if err != nil {
		return FetchResult{}, oops.In("embedded").Code("fetch_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "fetch messages")
	}
	messages := make([]Message, 0, len(poll.Records))
	for _, record := range poll.Records {
		messages = append(messages, messageFromRecord(topic, fetchOpts.partition, record))
	}
	return FetchResult{Messages: messages, NextOffset: poll.NextOffset, HighWatermark: poll.HighWatermark}, nil
}

func (b *Broker) Ack(ctx context.Context, consumer string, msg Message) error {
	return oops.In("embedded").Code("ack_failed").With("consumer", consumer, "topic", msg.Topic).Wrapf(
		b.broker.CommitOffset(ctx, consumer, msg.Topic, msg.Partition, msg.NextOffset),
		"ack message",
	)
}

func (b *Broker) Commit(ctx context.Context, consumer, topic string, partition uint32, nextOffset uint64) error {
	return oops.In("embedded").Code("commit_failed").With("consumer", consumer, "topic", topic).Wrapf(
		b.broker.CommitOffset(ctx, consumer, topic, partition, nextOffset),
		"commit offset",
	)
}

func (b *Broker) Nack(ctx context.Context, consumer string, msg Message, cause error) error {
	var lastError *string
	if cause != nil {
		value := cause.Error()
		lastError = &value
	}
	_, err := b.broker.Nack(ctx, consumer, msg.Topic, msg.Partition, msg.Offset, lastError)
	return oops.In("embedded").Code("nack_failed").With("consumer", consumer, "topic", msg.Topic).Wrapf(err, "nack message")
}

func (b *Broker) Schedule(ctx context.Context, topic string, payload []byte, deliverAt time.Time) (Message, error) {
	result, err := b.broker.ScheduleDelay(ctx, topic, 0, payload, uint64(deliverAt.UnixMilli()))
	if err != nil {
		return Message{}, oops.In("embedded").Code("schedule_failed").With("topic", topic).Wrapf(err, "schedule message")
	}
	return Message{Topic: result.DelayTopic, Partition: result.Partition, Offset: result.Offset, NextOffset: result.NextOffset, Payload: append([]byte(nil), payload...)}, nil
}

func stopScheduler(ctx context.Context, scheduled *internalbroker.ScheduledRuntime) error {
	if scheduled == nil {
		return nil
	}
	if err := scheduled.Stop(ctx); err != nil {
		return oops.In("embedded").Code("scheduler_stop_failed").Wrapf(err, "stop scheduler")
	}
	return nil
}

func stopInternalBroker(ctx context.Context, broker *internalbroker.Broker) error {
	if broker == nil {
		return nil
	}
	if err := broker.Stop(ctx); err != nil {
		return oops.In("embedded").Code("broker_stop_failed").Wrapf(err, "stop broker")
	}
	return nil
}

func closeMetadataStore(metaStore *store.StorxMetadataStore) error {
	if metaStore == nil {
		return nil
	}
	if err := metaStore.Close(); err != nil {
		return oops.In("embedded").Code("metadata_store_close_failed").Wrapf(err, "close metadata store")
	}
	return nil
}

func closeLogStore(logStore *store.StorxLogStore) error {
	if logStore == nil {
		return nil
	}
	if err := logStore.Close(); err != nil {
		return oops.In("embedded").Code("log_store_close_failed").Wrapf(err, "close log store")
	}
	return nil
}
