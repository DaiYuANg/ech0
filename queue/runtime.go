// Package queue contains topic queue runtime operations.
package queue

import (
	"github.com/DaiYuANg/ech0/store"
	"github.com/samber/oops"
)

type Runtime struct {
	log  store.MessageLogStore
	meta interface {
		store.OffsetStore
		store.TopicCatalogStore
	}
}

func New(log store.MessageLogStore, meta interface {
	store.OffsetStore
	store.TopicCatalogStore
}) *Runtime {
	return &Runtime{log: log, meta: meta}
}

func (r *Runtime) CreateTopic(topic store.TopicConfig) error {
	if err := r.log.CreateTopic(topic); err != nil {
		return oops.In("queue").Code("create_log_topic_failed").With("topic", topic.Name).Wrapf(err, "create log topic")
	}
	return oops.In("queue").Code("save_topic_config_failed").With("topic", topic.Name).Wrapf(r.meta.SaveTopicConfig(topic), "save topic config")
}

func (r *Runtime) Publish(topic string, partition uint32, payload []byte) (store.Record, error) {
	return r.PublishRecord(topic, partition, store.NewRecordAppend(payload))
}

func (r *Runtime) PublishRecord(topic string, partition uint32, record store.RecordAppend) (store.Record, error) {
	out, err := r.log.AppendRecord(store.NewTopicPartition(topic, partition), record)
	if err != nil {
		return store.Record{}, oops.In("queue").Code("append_record_failed").With("topic", topic, "partition", partition).Wrapf(err, "append record")
	}
	return out, nil
}

func (r *Runtime) PublishBatchRecords(topic string, partition uint32, records []store.RecordAppend) ([]store.Record, error) {
	out, err := r.log.AppendRecordsBatch(store.NewTopicPartition(topic, partition), records)
	if err != nil {
		return nil, oops.In("queue").Code("append_records_failed").With("topic", topic, "partition", partition).Wrapf(err, "append records")
	}
	return out, nil
}

func (r *Runtime) Fetch(consumer, topic string, partition uint32, offset *uint64, maxRecords int) (store.PollResult, error) {
	tp := store.NewTopicPartition(topic, partition)
	nextOffset := uint64(0)
	if offset != nil {
		nextOffset = *offset
	} else if committed, err := r.meta.LoadConsumerOffset(consumer, tp); err != nil {
		return store.PollResult{}, oops.In("queue").Code("load_consumer_offset_failed").With("consumer", consumer, "topic", topic, "partition", partition).Wrapf(err, "load consumer offset")
	} else if committed != nil {
		nextOffset = *committed
	}

	records, err := r.log.ReadFrom(tp, nextOffset, maxRecords)
	if err != nil {
		return store.PollResult{}, oops.In("queue").Code("read_records_failed").With("topic", topic, "partition", partition).Wrapf(err, "read records")
	}
	highWatermark, err := r.log.LastOffset(tp)
	if err != nil {
		return store.PollResult{}, oops.In("queue").Code("load_high_watermark_failed").With("topic", topic, "partition", partition).Wrapf(err, "load high watermark")
	}
	computedNext := nextOffset
	if len(records) > 0 {
		computedNext = records[len(records)-1].Offset + 1
	}
	return store.PollResult{
		Records:       records,
		NextOffset:    computedNext,
		HighWatermark: highWatermark,
	}, nil
}

func (r *Runtime) Ack(consumer, topic string, partition uint32, nextOffset uint64) error {
	return oops.In("queue").Code("save_consumer_offset_failed").With("consumer", consumer, "topic", topic, "partition", partition).Wrapf(
		r.meta.SaveConsumerOffset(consumer, store.NewTopicPartition(topic, partition), nextOffset),
		"save consumer offset",
	)
}

func (r *Runtime) ListTopics() ([]store.TopicConfig, error) {
	topics, err := r.meta.ListTopics()
	if err != nil {
		return nil, oops.In("queue").Code("list_topics_failed").Wrapf(err, "list topics")
	}
	return topics, nil
}
