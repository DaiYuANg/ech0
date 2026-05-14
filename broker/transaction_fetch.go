package broker

import (
	"context"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) FetchWithIsolation(
	ctx context.Context,
	consumer string,
	topic string,
	partition uint32,
	offset *uint64,
	maxRecords int,
	isolation FetchIsolation,
) (poll store.PollResult, err error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, topic)); err != nil {
		return store.PollResult{}, err
	}
	if err := b.checkQuota(ctx, QuotaRequest{Identity: identity, Action: QuotaActionConsume, Topic: topic, Records: maxRecords}); err != nil {
		return store.PollResult{}, err
	}
	return b.fetchWithIsolationScoped(ctx, scopedName(identity, "consumer", consumer), scopedTopicName(identity, topic), partition, offset, maxRecords, isolation)
}

func (b *Broker) fetchWithIsolationScoped(
	ctx context.Context,
	consumer string,
	topic string,
	partition uint32,
	offset *uint64,
	maxRecords int,
	isolation FetchIsolation,
) (poll store.PollResult, err error) {
	if isolation != FetchIsolationReadCommitted {
		return b.fetchScoped(ctx, consumer, topic, partition, offset, maxRecords)
	}
	const operation = "fetch_read_committed"
	totalStart := time.Now()
	defer func() {
		b.recordFetchStage(ctx, operation, "total", len(poll.Records), totalStart, err)
	}()
	if maxRecords <= 0 || maxRecords > b.cfg.Broker.MaxFetchRecords {
		maxRecords = b.cfg.Broker.MaxFetchRecords
	}
	paused, isPaused, err := b.pausedPollResult(consumer, topic, partition, offset)
	if err != nil || isPaused {
		return paused, err
	}
	nextOffset, err := b.fetchStartOffset(consumer, topic, partition, offset)
	if err != nil {
		return store.PollResult{}, err
	}
	records, committedNextOffset, err := b.readCommittedRecords(topic, partition, nextOffset, maxRecords)
	if err != nil {
		return store.PollResult{}, err
	}
	highWatermark, err := b.queue.LastOffset(store.NewTopicPartition(topic, partition))
	if err != nil {
		return store.PollResult{}, wrapBrokerStore(err, "load message high watermark")
	}
	return store.PollResult{Records: records, NextOffset: committedNextOffset, HighWatermark: highWatermark}, nil
}

func (b *Broker) fetchStartOffset(consumer, topic string, partition uint32, offset *uint64) (uint64, error) {
	if offset != nil {
		return *offset, nil
	}
	committed, err := b.meta.LoadConsumerOffset(consumer, store.NewTopicPartition(topic, partition))
	if err != nil {
		return 0, wrapBrokerStore(err, "load consumer offset")
	}
	if committed == nil {
		return 0, nil
	}
	return *committed, nil
}

func (b *Broker) readCommittedRecords(topic string, partition uint32, offset uint64, maxRecords int) ([]store.Record, uint64, error) {
	tp := store.NewTopicPartition(topic, partition)
	cursor := offset
	visible := collectionlist.NewListWithCapacity[store.Record](maxRecords)
	for visible.Len() < maxRecords {
		records, err := b.queue.ReadFrom(tp, cursor, maxRecords-visible.Len())
		if err != nil {
			return nil, cursor, wrapBrokerStore(err, "read committed messages")
		}
		if len(records) == 0 {
			return visible.Values(), cursor, nil
		}
		stop, nextCursor, err := b.collectCommittedRecords(records, visible, maxRecords)
		if err != nil {
			return nil, cursor, err
		}
		cursor = nextCursor
		if stop {
			return visible.Values(), cursor, nil
		}
	}
	return visible.Values(), cursor, nil
}

func (b *Broker) collectCommittedRecords(
	records []store.Record,
	visible *collectionlist.List[store.Record],
	maxRecords int,
) (bool, uint64, error) {
	nextCursor := uint64(0)
	for _, record := range records {
		nextCursor = record.Offset + 1
		visibility, err := b.transactionRecordVisibility(record)
		if err != nil {
			return false, nextCursor, err
		}
		switch visibility {
		case transactionRecordVisible:
			visible.Add(record)
		case transactionRecordHidden:
		case transactionRecordOpen:
			return true, record.Offset, nil
		}
		if visible.Len() >= maxRecords {
			return true, nextCursor, nil
		}
	}
	return false, nextCursor, nil
}

type transactionRecordVisibility uint8

const (
	transactionRecordVisible transactionRecordVisibility = iota
	transactionRecordHidden
	transactionRecordOpen
)

func (b *Broker) transactionRecordVisibility(record store.Record) (transactionRecordVisibility, error) {
	if record.Transaction == nil {
		return transactionRecordVisible, nil
	}
	if record.Transaction.ControlType != store.TransactionControlNone {
		return transactionRecordHidden, nil
	}
	state, err := b.meta.LoadTransaction(record.Transaction.TxID)
	if err != nil {
		return transactionRecordHidden, wrapBrokerStore(err, "load transaction visibility")
	}
	if state == nil || state.Status == store.TransactionStatusOpen {
		return transactionRecordOpen, nil
	}
	if state.Status == store.TransactionStatusCommitted {
		return transactionRecordVisible, nil
	}
	return transactionRecordHidden, nil
}
