package ech0

import (
	"context"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type DLQHeaderFilter = internalbroker.DLQHeaderFilter
type DLQQuery = internalbroker.DLQQuery

type DLQRecord struct {
	DLQTopic          string
	DLQPartition      uint32
	DLQOffset         uint64
	DLQNextOffset     uint64
	Message           Message
	OriginalTopic     string
	OriginalPartition uint32
	OriginalOffset    uint64
	RetryCount        uint32
	ErrorCode         string
	ErrorMessage      string
}

type DLQQueryResult struct {
	DLQTopic      string
	Partition     uint32
	Offset        uint64
	NextOffset    uint64
	HasMore       bool
	HighWatermark *uint64
	Records       []DLQRecord
}

type DLQReplayResult struct {
	DLQTopic     string
	DLQPartition uint32
	DLQOffset    uint64
	Message      Message
}

type DLQBulkReplayResult struct {
	DLQTopic   string
	Partition  uint32
	Replayed   []DLQReplayResult
	NextOffset uint64
	HasMore    bool
}

func (b *Broker) QueryDLQ(ctx context.Context, sourceTopic string, query DLQQuery) (DLQQueryResult, error) {
	query.SourceTopic = sourceTopic
	result, err := b.broker.QueryDLQ(ctx, query)
	if err != nil {
		return DLQQueryResult{}, oops.In("embedded").Code("query_dlq_failed").With("topic", sourceTopic).Wrapf(err, "query dlq")
	}
	return dlqQueryResultFromBroker(result), nil
}

func (b *Broker) ReplayDLQ(ctx context.Context, sourceTopic string, partition uint32, offset uint64) (DLQReplayResult, error) {
	result, err := b.broker.ReplayDLQ(ctx, sourceTopic, partition, offset)
	if err != nil {
		return DLQReplayResult{}, oops.In("embedded").Code("replay_dlq_failed").With("topic", sourceTopic).Wrapf(err, "replay dlq")
	}
	return dlqReplayResultFromBroker(result), nil
}

func (b *Broker) ReplayDLQQuery(ctx context.Context, sourceTopic string, query DLQQuery) (DLQBulkReplayResult, error) {
	query.SourceTopic = sourceTopic
	result, err := b.broker.ReplayDLQQuery(ctx, query)
	if err != nil {
		return DLQBulkReplayResult{}, oops.In("embedded").Code("replay_dlq_query_failed").With("topic", sourceTopic).Wrapf(err, "replay dlq query")
	}
	replayed := make([]DLQReplayResult, 0, len(result.Replayed))
	for index := range result.Replayed {
		item := result.Replayed[index]
		replayed = append(replayed, dlqReplayResultFromBroker(item))
	}
	return DLQBulkReplayResult{
		DLQTopic:   result.DLQTopic,
		Partition:  result.Partition,
		Replayed:   replayed,
		NextOffset: result.NextOffset,
		HasMore:    result.HasMore,
	}, nil
}

func (b *Broker) InspectPoisonMessage(ctx context.Context, topic string, partition uint32, offset uint64) (Message, error) {
	result, err := b.broker.InspectPoisonMessage(ctx, topic, partition, offset)
	if err != nil {
		return Message{}, oops.In("embedded").Code("inspect_poison_failed").With("topic", topic).Wrapf(err, "inspect poison message")
	}
	if result.Record == nil {
		return Message{}, nil
	}
	return messageFromRecord(topic, partition, *result.Record), nil
}

func (b *Broker) SkipPoisonMessage(ctx context.Context, consumer, topic string, partition uint32, offset uint64) error {
	_, err := b.broker.SkipPoisonMessage(ctx, consumer, topic, partition, offset)
	return oops.In("embedded").Code("skip_poison_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "skip poison message")
}

func (b *Broker) IsolatePoisonMessage(ctx context.Context, consumer, topic string, partition uint32, offset uint64, reason string) (DLQRecord, error) {
	result, err := b.broker.IsolatePoisonMessage(ctx, consumer, topic, partition, offset, reason)
	if err != nil {
		return DLQRecord{}, oops.In("embedded").Code("isolate_poison_failed").With("consumer", consumer, "topic", topic).Wrapf(err, "isolate poison message")
	}
	if result.DeadLetter == nil {
		return DLQRecord{}, nil
	}
	return dlqRecordFromBroker(*result.DeadLetter), nil
}

func (b *Broker) ReplayPoisonMessage(ctx context.Context, sourceTopic string, dlqPartition uint32, dlqOffset uint64) (DLQReplayResult, error) {
	result, err := b.broker.ReplayPoisonMessage(ctx, sourceTopic, dlqPartition, dlqOffset)
	if err != nil {
		return DLQReplayResult{}, oops.In("embedded").Code("replay_poison_failed").With("topic", sourceTopic).Wrapf(err, "replay poison message")
	}
	if result.ReplayResult == nil {
		return DLQReplayResult{}, nil
	}
	return dlqReplayResultFromBroker(*result.ReplayResult), nil
}

func dlqQueryResultFromBroker(result internalbroker.DLQQueryResult) DLQQueryResult {
	records := make([]DLQRecord, 0, len(result.Records))
	for index := range result.Records {
		record := result.Records[index]
		records = append(records, dlqRecordFromBroker(record))
	}
	return DLQQueryResult{
		DLQTopic:      result.DLQTopic,
		Partition:     result.Partition,
		Offset:        result.Offset,
		NextOffset:    result.NextOffset,
		HasMore:       result.HasMore,
		HighWatermark: result.HighWatermark,
		Records:       records,
	}
}

func dlqRecordFromBroker(record internalbroker.DLQRecord) DLQRecord {
	return DLQRecord{
		DLQTopic:          record.DLQTopic,
		DLQPartition:      record.DLQPartition,
		DLQOffset:         record.DLQOffset,
		DLQNextOffset:     record.DLQNextOffset,
		Message:           messageFromDLQRecord(record),
		OriginalTopic:     record.OriginalTopic,
		OriginalPartition: record.OriginalPartition,
		OriginalOffset:    record.OriginalOffset,
		RetryCount:        record.RetryCount,
		ErrorCode:         record.ErrorCode,
		ErrorMessage:      record.ErrorMessage,
	}
}

func dlqReplayResultFromBroker(result internalbroker.DLQReplayResult) DLQReplayResult {
	return DLQReplayResult{
		DLQTopic:     result.DLQTopic,
		DLQPartition: result.DLQPartition,
		DLQOffset:    result.DLQOffset,
		Message: Message{
			Topic:      result.Topic,
			Partition:  result.Partition,
			Offset:     result.Offset,
			NextOffset: result.NextOffset,
		},
	}
}

func messageFromDLQRecord(record internalbroker.DLQRecord) Message {
	return Message{
		Topic:      record.OriginalTopic,
		Partition:  record.OriginalPartition,
		Offset:     record.OriginalOffset,
		Timestamp:  time.UnixMilli(unixMillis(record.TimestampMS)),
		Key:        append([]byte(nil), record.Key...),
		Headers:    headersFromStore(record.Headers),
		Payload:    append([]byte(nil), record.Payload...),
		NextOffset: record.OriginalOffset + 1,
	}
}
