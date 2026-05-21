package broker

import (
	"bytes"
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
	topic.PriorityPolicy = store.NormalizeTopicPriorityPolicy(topic.PriorityPolicy)
}

func validateTopicPolicies(topic store.TopicConfig) error {
	switch topic.OrderingPolicy {
	case store.TopicOrderingNone, store.TopicOrderingPartition, store.TopicOrderingKey, store.TopicOrderingRoutingKey:
		return validateTopicPriorityPolicy(topic)
	default:
		return brokerStoreError(store.CodeInvalidArgument, "topic %s has invalid ordering policy %q", topic.Name, topic.OrderingPolicy)
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

func (b *Broker) resolveTopicOrderingPartitioning(
	topic store.TopicConfig,
	partitioning PublishPartitioning,
	records []store.RecordAppend,
) (PublishPartitioning, error) {
	switch topic.OrderingPolicy {
	case store.TopicOrderingNone, store.TopicOrderingPartition:
		return withResolvedRoutingKey(partitioning, records), nil
	case store.TopicOrderingKey:
		if _, ok := singleOrderingKey(records); !ok {
			return PublishPartitioning{}, brokerStoreError(store.CodeInvalidArgument, "topic %s requires one non-empty key per ordered batch", topic.Name)
		}
		partitioning.Mode = PartitionKeyHash
		return partitioning, nil
	case store.TopicOrderingRoutingKey:
		routingKey, ok := singleOrderingRoutingKey(partitioning, records)
		if !ok {
			return PublishPartitioning{}, brokerStoreError(store.CodeInvalidArgument, "topic %s requires one non-empty routing key per ordered batch", topic.Name)
		}
		partitioning.Mode = PartitionRoutingKeyHash
		partitioning.RoutingKey = routingKey
		return partitioning, nil
	default:
		return PublishPartitioning{}, brokerStoreError(store.CodeInvalidArgument, "topic %s has invalid ordering policy %q", topic.Name, topic.OrderingPolicy)
	}
}

func singleOrderingKey(records []store.RecordAppend) ([]byte, bool) {
	first := firstRecordKey(records)
	if len(first) == 0 {
		return nil, false
	}
	for index := range records {
		key := records[index].Key
		if len(key) == 0 || !bytes.Equal(key, first) {
			return nil, false
		}
	}
	return first, true
}

func singleOrderingRoutingKey(partitioning PublishPartitioning, records []store.RecordAppend) (string, bool) {
	first := partitionRoutingKey(partitioning, records)
	if first == "" {
		return "", false
	}
	for index := range records {
		routingKey := appendRoutingKey(records[index])
		if routingKey != "" && routingKey != first {
			return "", false
		}
	}
	return first, true
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
	out.ExpiresAtMS = cloneUint64Ptr(record.ExpiresAtMS)
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
			RoutingKey:  recordRoutingKey(record),
			Key:         append([]byte(nil), record.Key...),
			Headers:     protocolHeadersFromStore(record.Headers),
			Tombstone:   record.IsTombstone(),
			ExpiresAtMS: cloneUint64Ptr(record.ExpiresAtMS),
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

func cloneUint64Ptr(value *uint64) *uint64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
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
