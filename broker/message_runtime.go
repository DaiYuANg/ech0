package broker

import (
	"github.com/DaiYuANg/ech0/store"
)

type messageRuntime interface {
	CreateTopic(store.TopicConfig) error
	PublishRecord(string, uint32, store.RecordAppend) (store.Record, error)
	PublishBatchRecords(string, uint32, []store.RecordAppend) ([]store.Record, error)
	Fetch(string, string, uint32, *uint64, int) (store.PollResult, error)
	Ack(string, string, uint32, uint64) error
	ListTopics() ([]store.TopicConfig, error)
}
