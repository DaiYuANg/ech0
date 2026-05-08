package main

import (
	"context"
	"fmt"

	ech0 "github.com/DaiYuANg/ech0"
)

type benchBroker interface {
	CreateTopic(ctx context.Context, name string, partitions uint32) error
	Publish(ctx context.Context, topic string, payload, key []byte) (benchMessage, error)
	PublishBatch(ctx context.Context, topic string, partition uint32, records []benchPublishRecord) ([]benchMessage, error)
	Fetch(ctx context.Context, consumer, topic string, partition uint32, maxRecords int) (benchFetchResult, error)
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
	mq *ech0.Broker
}

func (b *embeddedBenchBroker) CreateTopic(ctx context.Context, name string, partitions uint32) error {
	if err := b.mq.CreateTopic(ctx, name, ech0.Partitions(partitions)); err != nil {
		return fmt.Errorf("embedded create topic: %w", err)
	}
	return nil
}

func (b *embeddedBenchBroker) Publish(ctx context.Context, topic string, payload, key []byte) (benchMessage, error) {
	msg, err := b.mq.Publish(ctx, topic, payload, ech0.Key(key))
	if err != nil {
		return benchMessage{}, fmt.Errorf("embedded publish: %w", err)
	}
	return benchMessage{Payload: msg.Payload, NextOffset: msg.NextOffset}, nil
}

func (b *embeddedBenchBroker) PublishBatch(ctx context.Context, topic string, partition uint32, records []benchPublishRecord) ([]benchMessage, error) {
	payloads := make([][]byte, 0, len(records))
	for _, record := range records {
		payloads = append(payloads, record.Payload)
	}
	messages, err := b.mq.PublishBatch(ctx, topic, payloads, ech0.Partition(partition))
	if err != nil {
		return nil, fmt.Errorf("embedded publish batch: %w", err)
	}
	out := make([]benchMessage, 0, len(messages))
	for _, msg := range messages {
		out = append(out, benchMessage{Payload: msg.Payload, NextOffset: msg.NextOffset})
	}
	return out, nil
}

func (b *embeddedBenchBroker) Fetch(ctx context.Context, consumer, topic string, partition uint32, maxRecords int) (benchFetchResult, error) {
	batch, err := b.mq.Fetch(ctx, consumer, topic, ech0.FetchPartition(partition), ech0.FetchLimit(maxRecords))
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
	if err := b.mq.Close(ctx); err != nil {
		return fmt.Errorf("embedded close: %w", err)
	}
	return nil
}
