package broker

import (
	"github.com/DaiYuANg/ech0/queue"
	"github.com/DaiYuANg/ech0/store"
)

type messageRuntime interface {
	CreateTopic(store.TopicConfig) error
	TopicExists(string) (bool, error)
	PublishRecord(string, uint32, store.RecordAppend) (store.Record, error)
	PublishBatchRecords(string, uint32, []store.RecordAppend) ([]store.Record, error)
	Fetch(string, string, uint32, *uint64, int) (store.PollResult, error)
	Ack(string, string, uint32, uint64) error
	ListTopics() ([]store.TopicConfig, error)
	ReadFrom(store.TopicPartition, uint64, int) ([]store.Record, error)
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

func (r singleMessageRuntime) CreateTopic(topic store.TopicConfig) error {
	return wrapBroker("message_topic_create_failed", r.queue.CreateTopic(topic), "create message topic %s", topic.Name)
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
