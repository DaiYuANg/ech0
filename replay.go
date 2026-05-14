package ech0

import (
	"context"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
)

type ReplayResult struct {
	Topic         string
	Partition     uint32
	Offset        uint64
	NextOffset    uint64
	Cursor        string
	NextCursor    string
	HasMore       bool
	HighWatermark *uint64
	Messages      []Message
}

type ReplayOption func(*replayOptions)

type replayOptions struct {
	partition  uint32
	maxRecords int
}

func ReplayPartition(partition uint32) ReplayOption {
	return func(opts *replayOptions) {
		opts.partition = partition
	}
}

func ReplayLimit(maxRecords int) ReplayOption {
	return func(opts *replayOptions) {
		if maxRecords > 0 {
			opts.maxRecords = maxRecords
		}
	}
}

func (b *Broker) ReplayFromOffset(ctx context.Context, topic string, offset uint64, opts ...ReplayOption) (ReplayResult, error) {
	replayOpts := buildReplayOptions(opts)
	result, err := b.broker.ReplayFromOffset(ctx, topic, replayOpts.partition, offset, replayOpts.maxRecords)
	if err != nil {
		return ReplayResult{}, oops.In("embedded").Code("replay_offset_failed").With("topic", topic).Wrapf(err, "replay messages from offset")
	}
	return replayResultFromBroker(result), nil
}

func (b *Broker) ReplayFromTimestamp(ctx context.Context, topic string, timestamp time.Time, opts ...ReplayOption) (ReplayResult, error) {
	replayOpts := buildReplayOptions(opts)
	result, err := b.broker.ReplayFromTimestamp(ctx, topic, replayOpts.partition, uint64(timestamp.UnixMilli()), replayOpts.maxRecords)
	if err != nil {
		return ReplayResult{}, oops.In("embedded").Code("replay_timestamp_failed").With("topic", topic).Wrapf(err, "replay messages from timestamp")
	}
	return replayResultFromBroker(result), nil
}

func (b *Broker) ReplayFromCursor(ctx context.Context, topic, cursor string, opts ...ReplayOption) (ReplayResult, error) {
	replayOpts := buildReplayOptions(opts)
	result, err := b.broker.ReplayFromCursor(ctx, topic, replayOpts.partition, cursor, replayOpts.maxRecords)
	if err != nil {
		return ReplayResult{}, oops.In("embedded").Code("replay_cursor_failed").With("topic", topic).Wrapf(err, "replay messages from cursor")
	}
	return replayResultFromBroker(result), nil
}

func buildReplayOptions(opts []ReplayOption) replayOptions {
	out := replayOptions{maxRecords: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&out)
		}
	}
	return out
}

func replayResultFromBroker(result internalbroker.ReplayResult) ReplayResult {
	return ReplayResult{
		Topic:         result.Topic,
		Partition:     result.Partition,
		Offset:        result.Offset,
		NextOffset:    result.NextOffset,
		Cursor:        result.Cursor,
		NextCursor:    result.NextCursor,
		HasMore:       result.HasMore,
		HighWatermark: cloneHighWatermark(result.HighWatermark),
		Messages:      recordsToMessages(result.Topic, result.Partition, result.Records),
	}
}

func cloneHighWatermark(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func recordsToMessages(topic string, partition uint32, records []store.Record) []Message {
	messages := make([]Message, 0, len(records))
	for _, record := range records {
		messages = append(messages, messageFromRecord(topic, partition, record))
	}
	return messages
}
