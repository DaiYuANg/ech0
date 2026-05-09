package broker

import (
	"github.com/DaiYuANg/ech0/queue"
	"github.com/DaiYuANg/ech0/store"
)

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

func (r singleMessageRuntime) EnforceRetention(nowMS uint64) (store.RetentionCleanupResult, error) {
	cleaner, ok := r.log.(store.RetentionCleaner)
	if !ok {
		return store.RetentionCleanupResult{}, nil
	}
	out, err := cleaner.EnforceRetention(nowMS)
	if err != nil {
		return store.RetentionCleanupResult{}, wrapBrokerStore(err, "enforce message retention")
	}
	return out, nil
}

func (r singleMessageRuntime) Compact(nowMS uint64, sealedSegmentBatch int) (store.CompactionCleanupResult, error) {
	cleaner, ok := r.log.(store.CompactionCleaner)
	if !ok {
		return store.CompactionCleanupResult{}, nil
	}
	out, err := cleaner.Compact(nowMS, sealedSegmentBatch)
	if err != nil {
		return store.CompactionCleanupResult{}, wrapBrokerStore(err, "compact messages")
	}
	return out, nil
}

func (r singleMessageRuntime) Close() error {
	return nil
}
