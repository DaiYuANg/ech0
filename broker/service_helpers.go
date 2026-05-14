package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/eventx"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
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
		Attributes:  record.Attributes,
		Payload:     append([]byte(nil), record.Payload...),
		Key:         append([]byte(nil), record.Key...),
		Transaction: cloneTransactionRecordMetadata(record.Transaction),
	}
	if record.TimestampMS != nil {
		v := *record.TimestampMS
		out.TimestampMS = &v
	}
	if len(record.Headers) > 0 {
		headers := collectionlist.NewListWithCapacity[store.RecordHeader](len(record.Headers))
		for _, header := range record.Headers {
			headers.Add(store.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)})
		}
		out.Headers = headers.Values()
	}
	return out
}

func fetchRecordsFromStore(records []store.Record) []protocol.FetchRecord {
	out := collectionlist.NewListWithCapacity[protocol.FetchRecord](len(records))
	for _, record := range records {
		out.Add(protocol.FetchRecord{
			Offset:      record.Offset,
			TimestampMS: record.TimestampMS,
			Key:         append([]byte(nil), record.Key...),
			Headers:     protocolHeadersFromStore(record.Headers),
			Tombstone:   record.IsTombstone(),
			Transaction: transactionRecordMetadataToProtocol(record.Transaction),
			Payload:     append([]byte(nil), record.Payload...),
		})
	}
	return out.Values()
}

func cloneTransactionRecordMetadata(metadata *store.TransactionRecordMetadata) *store.TransactionRecordMetadata {
	if metadata == nil {
		return nil
	}
	cp := *metadata
	return &cp
}

func transactionRecordMetadataToProtocol(metadata *store.TransactionRecordMetadata) *protocol.TransactionRecordMetadata {
	if metadata == nil {
		return nil
	}
	return &protocol.TransactionRecordMetadata{
		TxID:          metadata.TxID,
		ProducerID:    metadata.ProducerID,
		ProducerEpoch: metadata.ProducerEpoch,
		Sequence:      metadata.Sequence,
		ControlType:   protocol.TransactionControlType(metadata.ControlType),
	}
}

func (b *Broker) publishEvent(ctx context.Context, event eventx.Event) {
	if b.events == nil {
		return
	}
	if b.events.SubscriberCount() == 0 {
		return
	}
	if err := b.events.PublishAsync(ctx, event); err != nil && b.logger != nil {
		b.logger.Warn("publish event failed", "event", event.Name(), "error", err)
	}
}
