package ech0

import (
	"context"
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type DLQHeaderFilter struct {
	Key   string
	Value *string
}

type DLQQuery struct {
	Partition            uint32
	Offset               uint64
	MaxRecords           int
	FromTimestamp        *time.Time
	ToTimestamp          *time.Time
	OriginalPartition    *uint32
	OriginalOffset       *uint64
	ErrorCode            string
	ErrorMessageContains string
	Headers              []DLQHeaderFilter
}

type DLQRecord struct {
	DLQTopic          string
	DLQPartition      uint32
	DLQOffset         uint64
	DLQNextOffset     uint64
	OriginalTopic     string
	OriginalPartition uint32
	OriginalOffset    uint64
	RetryCount        uint32
	ErrorCode         string
	ErrorMessage      string
	Message           Message
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

func (b *Broker) QueryDLQ(ctx context.Context, sourceTopic string, query DLQQuery) (DLQQueryResult, error) {
	result, err := b.broker.QueryDLQ(ctx, dlqQueryToBroker(sourceTopic, query))
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
	}, nil
}

func dlqQueryToBroker(sourceTopic string, query DLQQuery) internalbroker.DLQQuery {
	return internalbroker.DLQQuery{
		SourceTopic:          sourceTopic,
		Partition:            query.Partition,
		Offset:               query.Offset,
		MaxRecords:           query.MaxRecords,
		FromTimestampMS:      optionalUnixMillis(query.FromTimestamp),
		ToTimestampMS:        optionalUnixMillis(query.ToTimestamp),
		OriginalPartition:    query.OriginalPartition,
		OriginalOffset:       query.OriginalOffset,
		ErrorCode:            query.ErrorCode,
		ErrorMessageContains: query.ErrorMessageContains,
		Headers:              dlqHeaderFiltersToBroker(query.Headers),
	}
}

func optionalUnixMillis(value *time.Time) *uint64 {
	if value == nil {
		return nil
	}
	millis := value.UnixMilli()
	if millis <= 0 {
		out := uint64(0)
		return &out
	}
	out := uint64(millis)
	return &out
}

func dlqHeaderFiltersToBroker(filters []DLQHeaderFilter) []internalbroker.DLQHeaderFilter {
	if len(filters) == 0 {
		return nil
	}
	out := make([]internalbroker.DLQHeaderFilter, 0, len(filters))
	for _, filter := range filters {
		out = append(out, internalbroker.DLQHeaderFilter{Key: filter.Key, Value: filter.Value})
	}
	return out
}

func dlqQueryResultFromBroker(result internalbroker.DLQQueryResult) DLQQueryResult {
	return DLQQueryResult{
		DLQTopic:      result.DLQTopic,
		Partition:     result.Partition,
		Offset:        result.Offset,
		NextOffset:    result.NextOffset,
		HasMore:       result.HasMore,
		HighWatermark: cloneHighWatermark(result.HighWatermark),
		Records:       dlqRecordsFromBroker(result.Records),
	}
}

func dlqRecordsFromBroker(records []internalbroker.DLQRecord) []DLQRecord {
	if len(records) == 0 {
		return nil
	}
	out := make([]DLQRecord, 0, len(records))
	for index := range records {
		out = append(out, dlqRecordFromBroker(records[index]))
	}
	return out
}

func dlqRecordFromBroker(record internalbroker.DLQRecord) DLQRecord {
	return DLQRecord{
		DLQTopic:          record.DLQTopic,
		DLQPartition:      record.DLQPartition,
		DLQOffset:         record.DLQOffset,
		DLQNextOffset:     record.DLQNextOffset,
		OriginalTopic:     record.OriginalTopic,
		OriginalPartition: record.OriginalPartition,
		OriginalOffset:    record.OriginalOffset,
		RetryCount:        record.RetryCount,
		ErrorCode:         record.ErrorCode,
		ErrorMessage:      record.ErrorMessage,
		Message: Message{
			Topic:      record.DLQTopic,
			Partition:  record.DLQPartition,
			Offset:     record.DLQOffset,
			Timestamp:  time.UnixMilli(unixMillis(record.TimestampMS)),
			Key:        append([]byte(nil), record.Key...),
			Headers:    headersFromStore(record.Headers),
			Payload:    append([]byte(nil), record.Payload...),
			NextOffset: record.DLQNextOffset,
		},
	}
}
