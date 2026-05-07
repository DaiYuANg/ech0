package broker

import (
	"context"

	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/arcgolabs/eventx"
)

func normalizeTopicPolicies(topic *store.TopicConfig) {
	if topic.Name == "" {
		return
	}
	defaults := store.NewTopicConfig(topic.Name)
	if topic.Partitions == 0 {
		topic.Partitions = defaults.Partitions
	}
	if topic.SegmentMaxBytes == 0 {
		topic.SegmentMaxBytes = defaults.SegmentMaxBytes
	}
	if topic.IndexIntervalBytes == 0 {
		topic.IndexIntervalBytes = defaults.IndexIntervalBytes
	}
	if topic.RetentionMaxBytes == 0 {
		topic.RetentionMaxBytes = defaults.RetentionMaxBytes
	}
	if topic.CleanupPolicy == "" {
		topic.CleanupPolicy = defaults.CleanupPolicy
	}
	if topic.MaxMessageBytes == 0 {
		topic.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if topic.MaxBatchBytes == 0 {
		topic.MaxBatchBytes = defaults.MaxBatchBytes
	}
	if topic.RetryPolicy.MaxAttempts == 0 {
		topic.RetryPolicy = defaults.RetryPolicy
	}
}

func groupConsumer(group string) string {
	return "__group_offset/" + group
}

func firstRecordKey(records []store.RecordAppend) []byte {
	for _, record := range records {
		if len(record.Key) > 0 {
			return record.Key
		}
	}
	return nil
}

func cloneAppend(record store.RecordAppend) store.RecordAppend {
	out := store.RecordAppend{
		Attributes: record.Attributes,
		Payload:    append([]byte(nil), record.Payload...),
		Key:        append([]byte(nil), record.Key...),
	}
	if record.TimestampMS != nil {
		v := *record.TimestampMS
		out.TimestampMS = &v
	}
	if len(record.Headers) > 0 {
		out.Headers = make([]store.RecordHeader, len(record.Headers))
		for i, header := range record.Headers {
			out.Headers[i] = store.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)}
		}
	}
	return out
}

func fetchRecordsFromStore(records []store.Record) []protocol.FetchRecord {
	out := make([]protocol.FetchRecord, 0, len(records))
	for _, record := range records {
		out = append(out, protocol.FetchRecord{
			Offset:      record.Offset,
			TimestampMS: record.TimestampMS,
			Key:         append([]byte(nil), record.Key...),
			Tombstone:   record.IsTombstone(),
			Payload:     append([]byte(nil), record.Payload...),
		})
	}
	return out
}

func (b *Broker) publishEvent(ctx context.Context, event eventx.Event) {
	if b.events == nil {
		return
	}
	if err := b.events.Publish(ctx, event); err != nil && b.logger != nil {
		b.logger.Warn("publish event failed", "event", event.Name(), "error", err)
	}
}
