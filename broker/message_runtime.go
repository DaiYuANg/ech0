package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/queue"
	"github.com/lyonbrown4d/ech0/store"
)

func fallbackOffsetForTimestamp(log store.MessageLogStore, topicPartition store.TopicPartition, timestampMS uint64) (uint64, *uint64, error) {
	offsets, err := log.PartitionOffsets(topicPartition)
	if err != nil {
		return 0, nil, wrapBrokerStore(err, "load partition offsets for timestamp seek")
	}
	endOffset := offsets.NextOffset
	cursor := uint64(0)
	for cursor < endOffset {
		offset, matched, found, done, scanErr := fallbackOffsetForTimestampScan(log, topicPartition, cursor, timestampMS)
		if scanErr != nil {
			return 0, nil, wrapBrokerStore(scanErr, "scan message batch for timestamp seek")
		}
		if found {
			return offset, matched, nil
		}
		if done {
			return endOffset, nil, nil
		}
		cursor = offset
	}
	return endOffset, nil, nil
}

func fallbackOffsetForTimestampScan(
	log store.MessageLogStore,
	topicPartition store.TopicPartition,
	cursor uint64,
	timestampMS uint64,
) (nextCursor uint64, matched *uint64, found, done bool, err error) {
	records, readErr := log.ReadFrom(topicPartition, cursor, minTimestampSeekScanBatch)
	if readErr != nil {
		return 0, nil, false, false, wrapBrokerStore(readErr, "read timestamp scan records")
	}
	if len(records) == 0 {
		return cursor, nil, false, true, nil
	}
	for _, record := range records {
		if record.TimestampMS >= timestampMS {
			timestamp := record.TimestampMS
			return record.Offset, &timestamp, true, false, nil
		}
	}
	return records[len(records)-1].Offset + 1, nil, false, false, nil
}

type messageRuntime interface {
	store.Snapshotter
	CreateTopic(store.TopicConfig) error
	TopicExists(string) (bool, error)
	PublishRecord(string, uint32, store.RecordAppend) (store.Record, error)
	PublishBatchRecords(string, uint32, []store.RecordAppend) ([]store.Record, error)
	Fetch(string, string, uint32, *uint64, int) (store.PollResult, error)
	Ack(string, string, uint32, uint64) error
	ListTopics() ([]store.TopicConfig, error)
	ReadFrom(store.TopicPartition, uint64, int) ([]store.Record, error)
	LastOffset(store.TopicPartition) (*uint64, error)
	PartitionOffsets(store.TopicPartition) (store.PartitionOffsetState, error)
	OffsetForTimestamp(store.TopicPartition, uint64) (uint64, *uint64, error)
	StorageUsage(string) (uint64, error)
	Close() error
}

type messagePageRuntime interface {
	ReadPage(store.TopicPartition, string, int) (store.RecordPage, error)
}

type singleMessageRuntime struct {
	queue *queue.Runtime
	log   store.MessageLogStore
}

func newSingleMessageRuntime(logStore store.MessageLogStore, metaStore metadataStore) messageRuntime {
	return singleMessageRuntime{
		queue: queue.New(logStore, metaStore),
		log:   logStore,
	}
}

func newBrokerMessageRuntime(b *Broker, logStore store.MessageLogStore, metaStore metadataStore) (messageRuntime, error) {
	if shouldUseShardedMessageRuntime(b.cfg, logStore) {
		return newShardedMessageRuntime(b.cfg, b.shardSpecs, b.shards, metaStore, b.logger, b.metrics)
	}
	return newSingleMessageRuntime(logStore, metaStore), nil
}

func shouldUseShardedMessageRuntime(cfg Config, logStore store.MessageLogStore) bool {
	if cfg.Broker.DataShardCount <= 1 {
		return false
	}
	_, ok := logStore.(*store.StorxLogStore)
	return ok
}

func (b *Broker) closeMessageRuntime() error {
	if b == nil || b.queue == nil {
		return nil
	}
	return wrapBroker("message_runtime_close_failed", b.queue.Close(), "close message runtime")
}

func (r singleMessageRuntime) CreateTopic(topic store.TopicConfig) error {
	return wrapBroker("message_topic_create_failed", r.queue.CreateTopic(topic), "create message topic %s", topic.Name)
}

func (r singleMessageRuntime) Snapshot() (store.Snapshot, error) {
	snapshotter, ok := r.log.(store.Snapshotter)
	if !ok {
		return store.Snapshot{}, brokerStoreError(store.CodeInvalidArgument, "message log does not support snapshots")
	}
	out, err := snapshotter.Snapshot()
	if err != nil {
		return store.Snapshot{}, wrapBroker("message_snapshot_failed", err, "snapshot messages")
	}
	return out, nil
}

func (r singleMessageRuntime) Restore(snapshot store.Snapshot) error {
	snapshotter, ok := r.log.(store.Snapshotter)
	if !ok {
		return brokerStoreError(store.CodeInvalidArgument, "message log does not support snapshots")
	}
	return wrapBroker("message_restore_failed", snapshotter.Restore(snapshot), "restore messages")
}

func (r singleMessageRuntime) TopicExists(topic string) (bool, error) {
	exists, err := r.log.TopicExists(topic)
	if err != nil {
		return false, wrapBrokerStore(err, "check message topic")
	}
	return exists, nil
}

func (r singleMessageRuntime) PublishRecord(topic string, partition uint32, record store.RecordAppend) (store.Record, error) {
	out, err := r.queue.PublishRecord(topic, partition, record)
	if err != nil {
		return store.Record{}, wrapBroker("message_record_publish_failed", err, "publish message record")
	}
	return out, nil
}

func (r singleMessageRuntime) PublishBatchRecords(topic string, partition uint32, records []store.RecordAppend) ([]store.Record, error) {
	out, err := r.queue.PublishBatchRecords(topic, partition, records)
	if err != nil {
		return nil, wrapBroker("message_records_publish_failed", err, "publish message records")
	}
	return out, nil
}

func (r singleMessageRuntime) Fetch(consumer, topic string, partition uint32, offset *uint64, maxRecords int) (store.PollResult, error) {
	out, err := r.queue.Fetch(consumer, topic, partition, offset, maxRecords)
	if err != nil {
		return store.PollResult{}, wrapBroker("message_fetch_failed", err, "fetch messages")
	}
	return out, nil
}

func (r singleMessageRuntime) Ack(consumer, topic string, partition uint32, nextOffset uint64) error {
	return wrapBroker("message_ack_failed", r.queue.Ack(consumer, topic, partition, nextOffset), "ack messages")
}

func (r singleMessageRuntime) ListTopics() ([]store.TopicConfig, error) {
	out, err := r.queue.ListTopics()
	if err != nil {
		return nil, wrapBroker("message_topics_list_failed", err, "list message topics")
	}
	return out, nil
}

func (r singleMessageRuntime) ReadFrom(topicPartition store.TopicPartition, offset uint64, maxRecords int) ([]store.Record, error) {
	out, err := r.log.ReadFrom(topicPartition, offset, maxRecords)
	if err != nil {
		return nil, wrapBrokerStore(err, "read messages")
	}
	return out, nil
}

func (r singleMessageRuntime) LastOffset(topicPartition store.TopicPartition) (*uint64, error) {
	out, err := r.log.LastOffset(topicPartition)
	if err != nil {
		return nil, wrapBrokerStore(err, "load message high watermark")
	}
	return out, nil
}

func (r singleMessageRuntime) PartitionOffsets(topicPartition store.TopicPartition) (store.PartitionOffsetState, error) {
	out, err := r.log.PartitionOffsets(topicPartition)
	if err != nil {
		return store.PartitionOffsetState{}, wrapBrokerStore(err, "load message partition offsets")
	}
	return out, nil
}

func (r singleMessageRuntime) OffsetForTimestamp(topicPartition store.TopicPartition, timestampMS uint64) (uint64, *uint64, error) {
	if fastPath, ok := r.log.(store.MessageLogTimestampStore); ok {
		offset, matched, err := fastPath.OffsetForTimestamp(topicPartition, timestampMS)
		if err != nil {
			return 0, nil, wrapBrokerStore(err, "lookup message timestamp offset")
		}
		return offset, matched, nil
	}
	offset, matched, err := fallbackOffsetForTimestamp(r.log, topicPartition, timestampMS)
	if err != nil {
		return 0, nil, wrapBrokerStore(err, "lookup message timestamp offset")
	}
	return offset, matched, nil
}

func (r singleMessageRuntime) StorageUsage(topic string) (uint64, error) {
	reporter, ok := r.log.(store.StorageUsageReporter)
	if !ok {
		return 0, nil
	}
	usage, err := reporter.StorageUsage(topic)
	if err != nil {
		return 0, wrapBrokerStore(err, "load message storage usage")
	}
	return usage, nil
}

func (r singleMessageRuntime) ReadPage(topicPartition store.TopicPartition, cursor string, maxRecords int) (store.RecordPage, error) {
	pager, ok := r.log.(store.MessageLogPager)
	if !ok {
		return store.RecordPage{}, brokerStoreError(store.CodeInvalidArgument, "message log does not support paging")
	}
	out, err := pager.ReadPage(topicPartition, cursor, maxRecords)
	if err != nil {
		return store.RecordPage{}, wrapBrokerStore(err, "page messages")
	}
	return out, nil
}

func (r singleMessageRuntime) EnforceRetention(ctx context.Context, nowMS uint64) (store.RetentionCleanupResult, error) {
	cleaner, ok := r.log.(store.RetentionCleaner)
	if !ok {
		return store.RetentionCleanupResult{}, nil
	}
	out, err := cleaner.EnforceRetention(ctx, nowMS)
	if err != nil {
		return store.RetentionCleanupResult{}, wrapBrokerStore(err, "enforce message retention")
	}
	return out, nil
}

func (r singleMessageRuntime) Compact(ctx context.Context, nowMS uint64, sealedSegmentBatch int) (store.CompactionCleanupResult, error) {
	cleaner, ok := r.log.(store.CompactionCleaner)
	if !ok {
		return store.CompactionCleanupResult{}, nil
	}
	out, err := cleaner.Compact(ctx, nowMS, sealedSegmentBatch)
	if err != nil {
		return store.CompactionCleanupResult{}, wrapBrokerStore(err, "compact messages")
	}
	return out, nil
}

func (r singleMessageRuntime) Close() error {
	return nil
}
