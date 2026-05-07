package queue

import (
	"github.com/DaiYuANg/ech0/store"
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
		return err
	}
	return r.meta.SaveTopicConfig(topic)
}

func (r *Runtime) Publish(topic string, partition uint32, payload []byte) (store.Record, error) {
	return r.PublishRecord(topic, partition, store.NewRecordAppend(payload))
}

func (r *Runtime) PublishRecord(topic string, partition uint32, record store.RecordAppend) (store.Record, error) {
	return r.log.AppendRecord(store.NewTopicPartition(topic, partition), record)
}

func (r *Runtime) PublishBatchRecords(topic string, partition uint32, records []store.RecordAppend) ([]store.Record, error) {
	return r.log.AppendRecordsBatch(store.NewTopicPartition(topic, partition), records)
}

func (r *Runtime) Fetch(consumer string, topic string, partition uint32, offset *uint64, maxRecords int) (store.PollResult, error) {
	tp := store.NewTopicPartition(topic, partition)
	nextOffset := uint64(0)
	if offset != nil {
		nextOffset = *offset
	} else if committed, err := r.meta.LoadConsumerOffset(consumer, tp); err != nil {
		return store.PollResult{}, err
	} else if committed != nil {
		nextOffset = *committed
	}

	records, err := r.log.ReadFrom(tp, nextOffset, maxRecords)
	if err != nil {
		return store.PollResult{}, err
	}
	highWatermark, err := r.log.LastOffset(tp)
	if err != nil {
		return store.PollResult{}, err
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

func (r *Runtime) Ack(consumer string, topic string, partition uint32, nextOffset uint64) error {
	return r.meta.SaveConsumerOffset(consumer, store.NewTopicPartition(topic, partition), nextOffset)
}

func (r *Runtime) ListTopics() ([]store.TopicConfig, error) {
	return r.meta.ListTopics()
}
