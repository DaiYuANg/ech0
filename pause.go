package ech0

import (
	"context"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type PauseResult struct {
	Topic     string
	Partition uint32
	Paused    bool
	UpdatedAt time.Time
}

func (b *Broker) PauseConsumer(ctx context.Context, consumer, topic string, partition uint32) (PauseResult, error) {
	result, err := b.broker.PauseConsumer(ctx, consumer, topic, partition)
	if err != nil {
		return PauseResult{}, oops.In("embedded").Code("pause_consumer_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "pause consumer")
	}
	return pauseResultFromBroker(result), nil
}

func (b *Broker) ResumeConsumer(ctx context.Context, consumer, topic string, partition uint32) (PauseResult, error) {
	result, err := b.broker.ResumeConsumer(ctx, consumer, topic, partition)
	if err != nil {
		return PauseResult{}, oops.In("embedded").Code("resume_consumer_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "resume consumer")
	}
	return pauseResultFromBroker(result), nil
}

func pauseResultFromBroker(result internalbroker.PauseResult) PauseResult {
	return PauseResult{
		Topic:     result.Topic,
		Partition: result.Partition,
		Paused:    result.Paused,
		UpdatedAt: time.UnixMilli(unixMillis(result.UpdatedAtMS)),
	}
}
