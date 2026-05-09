package main

import (
	"context"
	"errors"
	"fmt"

	ech0 "github.com/DaiYuANg/ech0"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

type benchBroker interface {
	CreateTopic(ctx context.Context, name string, partitions uint32) error
	Publish(ctx context.Context, topic string, payload, key []byte) (benchMessage, error)
	PublishBatch(ctx context.Context, topic string, partition uint32, records []benchPublishRecord) ([]benchMessage, error)
	Fetch(ctx context.Context, consumer, topic string, partition uint32, offset *uint64, maxRecords int) (benchFetchResult, error)
	Commit(ctx context.Context, consumer, topic string, partition uint32, nextOffset uint64) error
	Close(ctx context.Context) error
}

type benchPublishRecord struct {
	Payload []byte
	Key     []byte
}

type benchMessage struct {
	Payload    []byte
	NextOffset uint64
}

type benchFetchResult struct {
	Messages   []benchMessage
	NextOffset uint64
}

type embeddedBenchBroker struct {
	mq       *ech0.Broker
	cfg      benchConfig
	producer *ech0.Producer
}

func (b *embeddedBenchBroker) CreateTopic(ctx context.Context, name string, partitions uint32) error {
	if err := b.mq.CreateTopic(ctx, name, ech0.Partitions(partitions)); err != nil {
		return fmt.Errorf("embedded create topic: %w", err)
	}
	producer, err := b.mq.NewProducer(ctx, name,
		ech0.ProducerBatchSize(b.cfg.batchSize),
		ech0.ProducerInFlight(int(b.cfg.producerInflight)),
	)
	if err != nil {
		return fmt.Errorf("embedded create producer: %w", err)
	}
	b.producer = producer
	return nil
}

func (b *embeddedBenchBroker) Publish(ctx context.Context, topic string, payload, key []byte) (benchMessage, error) {
	producer, err := b.requireProducer()
	if err != nil {
		return benchMessage{}, err
	}
	future, err := producer.Send(ctx, payload, ech0.Key(key))
	if err != nil {
		return benchMessage{}, fmt.Errorf("embedded enqueue publish: %w", err)
	}
	msg, err := future.Await(ctx)
	if err != nil {
		return benchMessage{}, fmt.Errorf("embedded await publish: %w", err)
	}
	return benchMessage{Payload: msg.Payload, NextOffset: msg.NextOffset}, nil
}

func (b *embeddedBenchBroker) PublishBatch(ctx context.Context, topic string, partition uint32, records []benchPublishRecord) ([]benchMessage, error) {
	_ = topic
	producer, err := b.requireProducer()
	if err != nil {
		return nil, err
	}
	futures := collectionlist.NewListWithCapacity[*ech0.ProduceFuture](len(records))
	for _, record := range records {
		future, err := producer.Send(ctx, record.Payload, ech0.Partition(partition), ech0.Key(record.Key))
		if err != nil {
			return nil, fmt.Errorf("embedded enqueue publish batch: %w", err)
		}
		futures.Add(future)
	}
	out := make([]benchMessage, 0, futures.Len())
	for _, future := range futures.Values() {
		msg, err := future.Await(ctx)
		if err != nil {
			return nil, fmt.Errorf("embedded await publish batch: %w", err)
		}
		out = append(out, benchMessage{Payload: msg.Payload, NextOffset: msg.NextOffset})
	}
	return out, nil
}

func (b *embeddedBenchBroker) Fetch(ctx context.Context, consumer, topic string, partition uint32, offset *uint64, maxRecords int) (benchFetchResult, error) {
	opts := collectionlist.NewList(ech0.FetchPartition(partition), ech0.FetchLimit(maxRecords))
	if offset != nil {
		opts.Add(ech0.FetchOffset(*offset))
	}
	batch, err := b.mq.Fetch(ctx, consumer, topic, opts.Values()...)
	if err != nil {
		return benchFetchResult{}, fmt.Errorf("embedded fetch: %w", err)
	}
	messages := make([]benchMessage, 0, len(batch.Messages))
	for _, msg := range batch.Messages {
		messages = append(messages, benchMessage{Payload: msg.Payload, NextOffset: msg.NextOffset})
	}
	return benchFetchResult{Messages: messages, NextOffset: batch.NextOffset}, nil
}

func (b *embeddedBenchBroker) Commit(ctx context.Context, consumer, topic string, partition uint32, nextOffset uint64) error {
	if err := b.mq.Commit(ctx, consumer, topic, partition, nextOffset); err != nil {
		return fmt.Errorf("embedded commit: %w", err)
	}
	return nil
}

func (b *embeddedBenchBroker) Close(ctx context.Context) error {
	var result error
	if b.producer != nil {
		result = errors.Join(result, b.producer.Close(ctx))
	}
	if err := b.mq.Close(ctx); err != nil {
		result = errors.Join(result, fmt.Errorf("embedded close: %w", err))
	}
	return result
}

func (b *embeddedBenchBroker) requireProducer() (*ech0.Producer, error) {
	if b.producer == nil {
		return nil, errors.New("embedded producer is not initialized")
	}
	return b.producer, nil
}
