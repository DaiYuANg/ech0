package ech0

import (
	"time"

	"github.com/lyonbrown4d/ech0/store"
)

type Message struct {
	Topic      string
	Partition  uint32
	Offset     uint64
	Timestamp  time.Time
	Key        []byte
	Payload    []byte
	NextOffset uint64
	Tombstone  bool
}

type FetchResult struct {
	Messages      []Message
	NextOffset    uint64
	HighWatermark *uint64
}

func messageFromRecord(topic string, partition uint32, record store.Record) Message {
	return Message{
		Topic:      topic,
		Partition:  partition,
		Offset:     record.Offset,
		Timestamp:  time.UnixMilli(unixMillis(record.TimestampMS)),
		Key:        append([]byte(nil), record.Key...),
		Payload:    append([]byte(nil), record.Payload...),
		NextOffset: record.Offset + 1,
		Tombstone:  record.IsTombstone(),
	}
}

func unixMillis(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}
