package broker

import (
	"strconv"
	"strings"

	"github.com/DaiYuANg/ech0/direct"
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionset "github.com/arcgolabs/collectionx/set"
)

const (
	internalRetryTopicPrefix = "__retry"
	internalDelayTopicPrefix = "__delay"
	internalDLQTopicPrefix   = "__dlq"

	retryHeaderOriginalTopic     = "x-retry-original-topic"
	retryHeaderOriginalPartition = "x-retry-original-partition"
	retryHeaderOriginalOffset    = "x-retry-original-offset"
	retryHeaderRetryCount        = "x-retry-count"
	retryHeaderDeliverAtMS       = "x-retry-deliver-at-ms"
	retryHeaderLastError         = "x-retry-last-error"
	retryHeaderFailedConsumer    = "x-retry-failed-consumer"

	delayHeaderDeliverAtMS     = "x-delay-deliver-at-ms"
	delayHeaderTargetTopic     = "x-delay-target-topic"
	delayHeaderTargetPartition = "x-delay-target-partition"
	dlqHeaderOriginalTopic     = "x-dlq-original-topic"
	dlqHeaderOriginalPartition = "x-dlq-original-partition"
	dlqHeaderOriginalOffset    = "x-dlq-original-offset"
	dlqHeaderRetryCount        = "x-dlq-retry-count"
	dlqHeaderErrorCode         = "x-dlq-error-code"
	dlqHeaderErrorMessage      = "x-dlq-error-message"
	dlqErrorCodeRetryExhausted = "retry_exhausted"
	raftCommandNack            = "nack"
	raftCommandProcessRetry    = "process_retry"
	raftCommandScheduleDelay   = "schedule_delay"
	raftCommandProcessDelay    = "process_delay"
)

type RetryResult struct {
	RetryTopic      string
	RetryPartition  uint32
	RetryOffset     uint64
	RetryNextOffset uint64
	RetryCount      uint32
	DeliverAtMS     uint64
}

type ProcessRetryResult struct {
	RetryTopic          string
	Partition           uint32
	MovedToOrigin       int
	MovedToDeadLetter   int
	CommittedNextOffset *uint64
}

type DelayScheduleResult struct {
	DelayTopic  string
	Partition   uint32
	Offset      uint64
	NextOffset  uint64
	DeliverAtMS uint64
}

func (b *Broker) readRecordAtOffset(tp store.TopicPartition, offset uint64) (store.Record, error) {
	records, err := b.log.ReadFrom(tp, offset, 1)
	if err != nil {
		return store.Record{}, wrapBrokerStore(err, "read record by offset")
	}
	if len(records) == 0 || records[0].Offset != offset {
		return store.Record{}, brokerStoreError(store.CodeInvalidArgument, "record %s/%d@%d not found", tp.Topic, tp.Partition, offset)
	}
	return records[0], nil
}

func (b *Broker) loadTopicConfig(topic string) (*store.TopicConfig, error) {
	cfg, err := b.topicConfig(topic)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, brokerStoreError(store.CodeTopicNotFound, "topic %s not found", topic)
	}
	return cfg, nil
}

func (b *Broker) ensureAuxTopic(source store.TopicConfig, auxTopic string) error {
	cfg := store.NewTopicConfig(auxTopic)
	cfg.Partitions = source.Partitions
	cfg.SegmentMaxBytes = source.SegmentMaxBytes
	cfg.IndexIntervalBytes = source.IndexIntervalBytes
	cfg.RetentionMaxBytes = source.RetentionMaxBytes
	cfg.RetentionMS = source.RetentionMS
	cfg.MaxMessageBytes = source.MaxMessageBytes
	cfg.MaxBatchBytes = source.MaxBatchBytes
	cfg.RetryPolicy = source.RetryPolicy
	cfg.DeadLetterTopic = source.DeadLetterTopic
	cfg.DelayEnabled = source.DelayEnabled
	cfg.CompactionEnabled = false
	exists, err := b.log.TopicExists(auxTopic)
	if err != nil {
		return wrapBrokerStore(err, "check auxiliary topic")
	}
	if exists {
		if err := b.meta.SaveTopicConfig(cfg); err != nil {
			return wrapBrokerStore(err, "save auxiliary topic config")
		}
		if err := b.ensureTopicShardPlacements(cfg); err != nil {
			return wrapBroker("auxiliary_topic_shard_placement_failed", err, "ensure auxiliary topic shard placement")
		}
		b.cacheTopicConfig(cfg)
		return nil
	}
	if err := b.queue.CreateTopic(cfg); err != nil && store.ErrorCode(err) != store.CodeTopicExists {
		return wrapBroker("auxiliary_topic_create_failed", err, "create auxiliary topic")
	}
	if err := b.ensureTopicShardPlacements(cfg); err != nil {
		return wrapBroker("auxiliary_topic_shard_placement_failed", err, "ensure auxiliary topic shard placement")
	}
	b.cacheTopicConfig(cfg)
	return nil
}

func retryTopicName(sourceTopic string) string {
	return internalRetryTopicPrefix + "." + sourceTopic
}

func delayTopicName(targetTopic string) string {
	return internalDelayTopicPrefix + "." + targetTopic
}

func dlqTopicName(sourceTopic string) string {
	return internalDLQTopicPrefix + "." + sourceTopic
}

func isInternalTopicName(topic string) bool {
	return direct.IsInternalTopicName(topic) || isRetryTopicName(topic) || isDelayTopicName(topic) || isDLQTopicName(topic)
}

func isRetryTopicName(topic string) bool {
	return hasInternalPrefix(topic, internalRetryTopicPrefix)
}

func isDelayTopicName(topic string) bool {
	return hasInternalPrefix(topic, internalDelayTopicPrefix)
}

func isDLQTopicName(topic string) bool {
	return hasInternalPrefix(topic, internalDLQTopicPrefix)
}

func hasInternalPrefix(topic, prefix string) bool {
	return topic == prefix || strings.HasPrefix(topic, prefix+".") || strings.HasPrefix(topic, prefix+"/")
}

func header(key, value string) store.RecordHeader {
	return store.RecordHeader{Key: key, Value: []byte(value)}
}

func headerString(headers []store.RecordHeader, key string) string {
	for _, header := range headers {
		if header.Key == key {
			return string(header.Value)
		}
	}
	return ""
}

func headerUint64(headers []store.RecordHeader, key string) (*uint64, error) {
	raw := headerString(headers, key)
	if raw == "" {
		var absent *uint64
		return absent, nil
	}
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil, brokerStoreError(store.CodeCodec, "invalid header %s: %v", key, err)
	}
	return &parsed, nil
}

func headerUint32(headers []store.RecordHeader, key string) (*uint32, error) {
	raw := headerString(headers, key)
	if raw == "" {
		var absent *uint32
		return absent, nil
	}
	parsed, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return nil, brokerStoreError(store.CodeCodec, "invalid header %s: %v", key, err)
	}
	value := uint32(parsed)
	return &value, nil
}

func upsertHeader(headers *[]store.RecordHeader, key, value string) {
	for i := range *headers {
		if (*headers)[i].Key == key {
			(*headers)[i].Value = []byte(value)
			return
		}
	}
	*headers = append(*headers, header(key, value))
}

func removeHeaders(headers []store.RecordHeader, keys ...string) []store.RecordHeader {
	remove := collectionset.NewSet(keys...)
	out := headers[:0]
	for _, header := range headers {
		if !remove.Contains(header.Key) {
			out = append(out, header)
		}
	}
	return out
}

func cloneAsAppend(record store.Record) store.RecordAppend {
	return store.RecordAppend{
		TimestampMS: &record.TimestampMS,
		Key:         append([]byte(nil), record.Key...),
		Headers:     cloneRecordHeaders(record.Headers),
		Attributes:  record.Attributes,
		Payload:     append([]byte(nil), record.Payload...),
	}
}

func cloneRecordHeaders(headers []store.RecordHeader) []store.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := collectionlist.NewListWithCapacity[store.RecordHeader](len(headers))
	for _, header := range headers {
		out.Add(store.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	return out.Values()
}

func stringValue(value *string, fallback string) string {
	if value == nil || *value == "" {
		return fallback
	}
	return *value
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
