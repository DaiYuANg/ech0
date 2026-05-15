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
	Headers    []Header
	Payload    []byte
	NextOffset uint64
	Tombstone  bool
}

type Header struct {
	Key   string
	Value []byte
}

type FetchResult struct {
	Messages       []Message
	NextOffset     uint64
	HighWatermark  *uint64
	LowWatermark   *uint64
	LogStartOffset uint64
}

func messageFromRecord(topic string, partition uint32, record store.Record) Message {
	return Message{
		Topic:      topic,
		Partition:  partition,
		Offset:     record.Offset,
		Timestamp:  time.UnixMilli(unixMillis(record.TimestampMS)),
		Key:        append([]byte(nil), record.Key...),
		Headers:    headersFromStore(record.Headers),
		Payload:    append([]byte(nil), record.Payload...),
		NextOffset: record.Offset + 1,
		Tombstone:  record.IsTombstone(),
	}
}

func headersFromStore(headers []store.RecordHeader) []Header {
	if len(headers) == 0 {
		return nil
	}
	out := make([]Header, 0, len(headers))
	for _, header := range headers {
		out = append(out, Header{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	return out
}

func headersToStore(headers []Header) []store.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	out := make([]store.RecordHeader, 0, len(headers))
	for _, header := range headers {
		out = append(out, store.RecordHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	return out
}

func unixMillis(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}
