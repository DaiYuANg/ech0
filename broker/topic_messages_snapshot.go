package broker

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) TopicMessagesSnapshot(topic string, partition uint32, offset uint64, limit int) (TopicMessagesPageSummary, error) {
	return b.TopicMessagesSnapshotFor(context.Background(), topic, partition, offset, limit)
}

func (b *Broker) TopicMessagesSnapshotFor(ctx context.Context, topic string, partition uint32, offset uint64, limit int) (TopicMessagesPageSummary, error) {
	if limit <= 0 || limit > b.cfg.Broker.MaxFetchRecords {
		limit = b.cfg.Broker.MaxFetchRecords
	}
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
		return TopicMessagesPageSummary{}, err
	}
	scopedTopic := scopedTopicName(identity, topic)
	tp := store.NewTopicPartition(scopedTopic, partition)
	offsets, err := b.queue.PartitionOffsets(tp)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_offsets_failed", err, "load topic message offsets")
	}
	effectiveOffset := max(offset, offsets.LogStartOffset)
	records, err := b.queue.ReadFrom(tp, effectiveOffset, limit)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_read_failed", err, "read topic messages")
	}
	items := collectionlist.NewList[TopicMessageSummary]()
	nextOffset := effectiveOffset
	for _, record := range records {
		items.Add(topicMessageSummary(record))
		nextOffset = record.Offset + 1
	}
	return TopicMessagesPageSummary{
		Topic:          visibleTopicName(identity, scopedTopic),
		Partition:      partition,
		Offset:         effectiveOffset,
		Limit:          limit,
		NextOffset:     nextOffset,
		HasMore:        len(records) == limit && offsets.HighWatermark != nil && nextOffset <= *offsets.HighWatermark,
		HighWatermark:  offsets.HighWatermark,
		LowWatermark:   offsets.LowWatermark,
		LogStartOffset: offsets.LogStartOffset,
		Records:        items.Values(),
	}, nil
}

func (b *Broker) TopicMessagesCursorSnapshot(topic string, partition uint32, cursor string, limit int) (TopicMessagesPageSummary, error) {
	return b.TopicMessagesCursorSnapshotFor(context.Background(), topic, partition, cursor, limit)
}

func (b *Broker) TopicMessagesCursorSnapshotFor(ctx context.Context, topic string, partition uint32, cursor string, limit int) (TopicMessagesPageSummary, error) {
	if limit <= 0 || limit > b.cfg.Broker.MaxFetchRecords {
		limit = b.cfg.Broker.MaxFetchRecords
	}
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
		return TopicMessagesPageSummary{}, err
	}
	scopedTopic := scopedTopicName(identity, topic)
	tp := store.NewTopicPartition(scopedTopic, partition)
	pager, ok := b.queue.(messagePageRuntime)
	if !ok {
		return b.TopicMessagesSnapshotFor(ctx, topic, partition, 0, limit)
	}
	page, err := pager.ReadPage(tp, cursor, limit)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_page_failed", err, "page topic messages")
	}
	offsets, err := b.queue.PartitionOffsets(tp)
	if err != nil {
		return TopicMessagesPageSummary{}, wrapBroker("topic_messages_offsets_failed", err, "load topic message offsets")
	}
	items := collectionlist.NewListWithCapacity[TopicMessageSummary](len(page.Records))
	nextOffset := offsets.LogStartOffset
	offset := offsets.LogStartOffset
	for idx, record := range page.Records {
		if idx == 0 {
			offset = record.Offset
		}
		items.Add(topicMessageSummary(record))
		nextOffset = record.Offset + 1
	}
	return TopicMessagesPageSummary{
		Topic:          visibleTopicName(identity, scopedTopic),
		Partition:      partition,
		Offset:         offset,
		Limit:          limit,
		Cursor:         cursor,
		NextOffset:     nextOffset,
		NextCursor:     page.NextCursor,
		HasMore:        page.HasMore,
		HighWatermark:  offsets.HighWatermark,
		LowWatermark:   offsets.LowWatermark,
		LogStartOffset: offsets.LogStartOffset,
		Records:        items.Values(),
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
