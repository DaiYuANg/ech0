package broker

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) TopicMessagesSnapshot(topic string, partition uint32, offset uint64, limit int) (TopicMessagesPageSummary, error) {
	if limit <= 0 || limit > b.cfg.Broker.MaxFetchRecords {
		limit = b.cfg.Broker.MaxFetchRecords
	}
	tp := store.NewTopicPartition(topic, partition)
	records, err := b.queue.ReadFrom(tp, offset, limit)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_read_failed", err, "read topic messages")
	}
	highWatermark, err := b.queue.LastOffset(tp)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_high_watermark_failed", err, "load topic message high watermark")
	}
	items := collectionlist.NewList[TopicMessageSummary]()
	nextOffset := offset
	for _, record := range records {
		items.Add(topicMessageSummary(record))
		nextOffset = record.Offset + 1
	}
	return TopicMessagesPageSummary{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		Limit:         limit,
		NextOffset:    nextOffset,
		HasMore:       len(records) == limit && highWatermark != nil && nextOffset <= *highWatermark,
		HighWatermark: highWatermark,
		Records:       items.Values(),
	}, nil
}

func (b *Broker) TopicMessagesCursorSnapshot(topic string, partition uint32, cursor string, limit int) (TopicMessagesPageSummary, error) {
	if limit <= 0 || limit > b.cfg.Broker.MaxFetchRecords {
		limit = b.cfg.Broker.MaxFetchRecords
	}
	tp := store.NewTopicPartition(topic, partition)
	pager, ok := b.queue.(messagePageRuntime)
	if !ok {
		return b.TopicMessagesSnapshot(topic, partition, 0, limit)
	}
	page, err := pager.ReadPage(tp, cursor, limit)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_page_failed", err, "page topic messages")
	}
	highWatermark, err := b.queue.LastOffset(tp)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_high_watermark_failed", err, "load topic message high watermark")
	}
	items := collectionlist.NewListWithCapacity[TopicMessageSummary](len(page.Records))
	nextOffset := uint64(0)
	offset := uint64(0)
	for idx, record := range page.Records {
		if idx == 0 {
			offset = record.Offset
		}
		items.Add(topicMessageSummary(record))
		nextOffset = record.Offset + 1
	}
	return TopicMessagesPageSummary{
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
		Limit:         limit,
		Cursor:        cursor,
		NextOffset:    nextOffset,
		NextCursor:    page.NextCursor,
		HasMore:       page.HasMore,
		HighWatermark: highWatermark,
		Records:       items.Values(),
	}, nil
}

func topicMessageSummary(record store.Record) TopicMessageSummary {
	preview := record.Payload
	if len(preview) > 96 {
		preview = preview[:96]
	}
	jsonPreview := compactJSONPreview(preview)
	return TopicMessageSummary{
		Offset:             record.Offset,
		TimestampMS:        record.TimestampMS,
		PayloadSize:        len(record.Payload),
		PayloadUTF8Preview: strings.TrimSpace(string(bytes.ToValidUTF8(preview, []byte(".")))),
		PayloadHexPreview:  hex.EncodeToString(preview),
		PayloadJSONPreview: jsonPreview,
	}
}

func compactJSONPreview(payload []byte) *string {
	if !json.Valid(payload) {
		return nil
	}
	var compacted bytes.Buffer
	if err := json.Compact(&compacted, payload); err != nil {
		return nil
	}
	value := compacted.String()
	if len(value) > 160 {
		value = value[:160]
	}
	return &value
}
