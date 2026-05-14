package ech0

import (
	"context"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type SeekResult struct {
	Topic     string
	Partition uint32
	Offset    uint64
	Timestamp *time.Time
}

func (b *Broker) SeekOffset(ctx context.Context, consumer, topic string, partition uint32, offset uint64) (SeekResult, error) {
	result, err := b.broker.SeekOffset(ctx, consumer, topic, partition, offset)
	if err != nil {
		return SeekResult{}, oops.In("embedded").Code("seek_offset_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "seek offset")
	}
	return seekResultFromBroker(result), nil
}

func (b *Broker) SeekTimestamp(ctx context.Context, consumer, topic string, partition uint32, timestamp time.Time) (SeekResult, error) {
	result, err := b.broker.SeekTimestamp(ctx, consumer, topic, partition, uint64(timestamp.UnixMilli()))
	if err != nil {
		return SeekResult{}, oops.In("embedded").Code("seek_timestamp_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "seek timestamp")
	}
	return seekResultFromBroker(result), nil
}

func seekResultFromBroker(result internalbroker.SeekResult) SeekResult {
	out := SeekResult{
		Topic:     result.Topic,
		Partition: result.Partition,
		Offset:    result.Offset,
	}
	if result.TimestampMS != nil {
		timestamp := time.UnixMilli(unixMillis(*result.TimestampMS))
		out.Timestamp = &timestamp
	}
	return out
}
