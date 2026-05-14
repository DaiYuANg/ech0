package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

type ReplayResult struct {
	Topic         string
	Partition     uint32
	Offset        uint64
	NextOffset    uint64
	Cursor        string
	NextCursor    string
	HasMore       bool
	HighWatermark *uint64
	Records       []store.Record
}

func (b *Broker) ReplayFromOffset(ctx context.Context, topic string, partition uint32, offset uint64, maxRecords int) (ReplayResult, error) {
	identity, scopedTopic, limit, err := b.prepareReplay(ctx, topic, maxRecords)
	if err != nil {
		return ReplayResult{}, err
	}
	return b.replayFromOffsetScoped(identity, scopedTopic, partition, offset, limit)
}

func (b *Broker) ReplayFromTimestamp(ctx context.Context, topic string, partition uint32, timestampMS uint64, maxRecords int) (ReplayResult, error) {
	identity, scopedTopic, limit, err := b.prepareReplay(ctx, topic, maxRecords)
	if err != nil {
		return ReplayResult{}, err
	}
	position, err := b.offsetForTimestamp(scopedTopic, partition, timestampMS)
	if err != nil {
		return ReplayResult{}, err
	}
	return b.replayFromOffsetScoped(identity, scopedTopic, partition, position.Offset, limit)
}

func (b *Broker) ReplayFromCursor(ctx context.Context, topic string, partition uint32, cursor string, maxRecords int) (ReplayResult, error) {
	identity, scopedTopic, limit, err := b.prepareReplay(ctx, topic, maxRecords)
	if err != nil {
		return ReplayResult{}, err
	}
	pager, ok := b.queue.(messagePageRuntime)
	if !ok {
		return ReplayResult{}, brokerStoreError(store.CodeInvalidArgument, "message runtime does not support replay cursors")
	}
	tp := store.NewTopicPartition(scopedTopic, partition)
	page, err := pager.ReadPage(tp, cursor, limit)
	if err != nil {
		return ReplayResult{}, wrapBrokerStore(err, "replay cursor messages")
	}
	highWatermark, err := b.queue.LastOffset(tp)
	if err != nil {
		return ReplayResult{}, wrapBrokerStore(err, "load replay high watermark")
	}
	return replayResultFromRecords(identity, scopedTopic, partition, cursor, page.NextCursor, page.HasMore, highWatermark, page.Records), nil
}

func (b *Broker) prepareReplay(ctx context.Context, topic string, maxRecords int) (Identity, string, int, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
		return Identity{}, "", 0, err
	}
	limit := maxRecords
	if limit <= 0 || limit > b.cfg.Broker.MaxFetchRecords {
		limit = b.cfg.Broker.MaxFetchRecords
	}
	return identity, scopedTopicName(identity, topic), limit, nil
}

func (b *Broker) replayFromOffsetScoped(identity Identity, topic string, partition uint32, offset uint64, maxRecords int) (ReplayResult, error) {
	if err := b.validateSeekTopicPartition(topic, partition); err != nil {
		return ReplayResult{}, err
	}
	tp := store.NewTopicPartition(topic, partition)
	records, err := b.queue.ReadFrom(tp, offset, maxRecords)
	if err != nil {
		return ReplayResult{}, wrapBrokerStore(err, "replay offset messages")
	}
	highWatermark, err := b.queue.LastOffset(tp)
	if err != nil {
		return ReplayResult{}, wrapBrokerStore(err, "load replay high watermark")
	}
	hasMore := replayHasMore(records, maxRecords, highWatermark)
	return replayResultFromRecords(identity, topic, partition, "", "", hasMore, highWatermark, records), nil
}

func replayResultFromRecords(
	identity Identity,
	topic string,
	partition uint32,
	cursor string,
	nextCursor string,
	hasMore bool,
	highWatermark *uint64,
	records []store.Record,
) ReplayResult {
	out := ReplayResult{
		Topic:         visibleTopicName(identity, topic),
		Partition:     partition,
		Cursor:        cursor,
		NextCursor:    nextCursor,
		HasMore:       hasMore,
		HighWatermark: highWatermark,
		Records:       replayRecords(records),
	}
	if len(records) == 0 {
		return out
	}
	out.Offset = records[0].Offset
	out.NextOffset = records[len(records)-1].Offset + 1
	return out
}

func replayRecords(records []store.Record) []store.Record {
	out := collectionlist.NewListWithCapacity[store.Record](len(records))
	for _, record := range records {
		out.Add(record)
	}
	return out.Values()
}

func replayHasMore(records []store.Record, maxRecords int, highWatermark *uint64) bool {
	if len(records) == 0 || len(records) < maxRecords || highWatermark == nil {
		return false
	}
	return records[len(records)-1].Offset < *highWatermark
}
