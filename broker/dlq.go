package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

type DLQHeaderFilter struct {
	Key   string
	Value *string
}

type DLQQuery struct {
	SourceTopic          string
	Partition            uint32
	Offset               uint64
	MaxRecords           int
	FromTimestampMS      *uint64
	ToTimestampMS        *uint64
	OriginalPartition    *uint32
	OriginalOffset       *uint64
	ErrorCode            string
	ErrorMessageContains string
	RetryCount           *uint32
	Headers              []DLQHeaderFilter
}

type DLQRecord struct {
	DLQTopic          string
	DLQPartition      uint32
	DLQOffset         uint64
	DLQNextOffset     uint64
	TimestampMS       uint64
	Key               []byte
	Headers           []store.RecordHeader
	Payload           []byte
	OriginalTopic     string
	OriginalPartition uint32
	OriginalOffset    uint64
	RetryCount        uint32
	ErrorCode         string
	ErrorMessage      string
}

type DLQQueryResult struct {
	DLQTopic      string
	Partition     uint32
	Offset        uint64
	NextOffset    uint64
	HasMore       bool
	HighWatermark *uint64
	Records       []DLQRecord
}

type DLQReplayResult struct {
	DLQTopic     string
	DLQPartition uint32
	DLQOffset    uint64
	Topic        string
	Partition    uint32
	Offset       uint64
	NextOffset   uint64
}

func (b *Broker) QueryDLQ(ctx context.Context, query DLQQuery) (DLQQueryResult, error) {
	identity, scopedSource, limit, err := b.prepareDLQQuery(ctx, query)
	if err != nil {
		return DLQQueryResult{}, err
	}
	dlqTopic, err := b.dlqTopicForSource(scopedSource)
	if err != nil {
		return DLQQueryResult{}, err
	}
	result, err := b.queryDLQScoped(dlqTopic, scopedSource, query, limit)
	if err != nil {
		return DLQQueryResult{}, err
	}
	result.DLQTopic = visibleTopicName(identity, result.DLQTopic)
	for index := range result.Records {
		result.Records[index].DLQTopic = visibleTopicName(identity, result.Records[index].DLQTopic)
		result.Records[index].OriginalTopic = visibleTopicName(identity, result.Records[index].OriginalTopic)
	}
	return result, nil
}

func (b *Broker) ReplayDLQ(ctx context.Context, sourceTopic string, partition uint32, offset uint64) (DLQReplayResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, sourceTopic)); err != nil {
		return DLQReplayResult{}, err
	}
	if err := b.authorize(ctx, identity, ACLActionProduce, topicResource(identity, sourceTopic)); err != nil {
		return DLQReplayResult{}, err
	}
	scopedSource := scopedTopicName(identity, sourceTopic)
	dlqTopic, err := b.dlqTopicForSource(scopedSource)
	if err != nil {
		return DLQReplayResult{}, err
	}
	record, err := b.readRecordAtOffset(store.NewTopicPartition(dlqTopic, partition), offset)
	if err != nil {
		return DLQReplayResult{}, err
	}
	dlqRecord, err := dlqRecordFromStore(dlqTopic, partition, record)
	if err != nil {
		return DLQReplayResult{}, err
	}
	if dlqRecord.OriginalTopic != scopedSource {
		return DLQReplayResult{}, brokerStoreError(store.CodeInvalidArgument, "dlq record belongs to %s, not %s", dlqRecord.OriginalTopic, scopedSource)
	}
	produced, err := b.PublishRecord(ctx, visibleTopicName(identity, dlqRecord.OriginalTopic), PublishPartitioning{
		Mode:      PartitionExplicit,
		Partition: dlqRecord.OriginalPartition,
	}, dlqReplayAppend(record))
	if err != nil {
		return DLQReplayResult{}, err
	}
	return DLQReplayResult{
		DLQTopic:     visibleTopicName(identity, dlqTopic),
		DLQPartition: partition,
		DLQOffset:    offset,
		Topic:        visibleTopicName(identity, dlqRecord.OriginalTopic),
		Partition:    produced.Partition,
		Offset:       produced.Record.Offset,
		NextOffset:   produced.Record.Offset + 1,
	}, nil
}

func (b *Broker) prepareDLQQuery(ctx context.Context, query DLQQuery) (Identity, string, int, error) {
	if query.SourceTopic == "" {
		return Identity{}, "", 0, brokerStoreError(store.CodeInvalidArgument, "source topic is required")
	}
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionConsume, topicResource(identity, query.SourceTopic)); err != nil {
		return Identity{}, "", 0, err
	}
	limit := query.MaxRecords
	if limit <= 0 || limit > b.cfg.Broker.MaxFetchRecords {
		limit = b.cfg.Broker.MaxFetchRecords
	}
	return identity, scopedTopicName(identity, query.SourceTopic), limit, nil
}

func (b *Broker) dlqTopicForSource(scopedSourceTopic string) (string, error) {
	sourceTopic, err := b.loadTopicConfig(scopedSourceTopic)
	if err != nil {
		return "", err
	}
	if sourceTopic.DeadLetterTopic != nil && *sourceTopic.DeadLetterTopic != "" {
		return *sourceTopic.DeadLetterTopic, nil
	}
	return dlqTopicName(scopedSourceTopic), nil
}

func (b *Broker) queryDLQScoped(dlqTopic, scopedSource string, query DLQQuery, limit int) (DLQQueryResult, error) {
	tp := store.NewTopicPartition(dlqTopic, query.Partition)
	exists, err := b.queue.TopicExists(dlqTopic)
	if err != nil {
		return DLQQueryResult{}, wrapBrokerStore(err, "check dlq topic")
	}
	if !exists {
		return DLQQueryResult{DLQTopic: dlqTopic, Partition: query.Partition, Offset: query.Offset, NextOffset: query.Offset}, nil
	}
	offsets, err := b.queue.PartitionOffsets(tp)
	if err != nil {
		return DLQQueryResult{}, wrapBrokerStore(err, "load dlq offsets")
	}
	indexed, ok, err := b.queryDLQByIndex(tp, scopedSource, query, limit, offsets)
	if err != nil {
		return DLQQueryResult{}, err
	}
	if ok {
		return indexed, nil
	}
	return b.scanDLQRecords(tp, query, limit, offsets)
}

func (b *Broker) scanDLQRecords(
	tp store.TopicPartition,
	query DLQQuery,
	limit int,
	offsets store.PartitionOffsetState,
) (DLQQueryResult, error) {
	start := max(query.Offset, offsets.LogStartOffset)
	nextOffset := start
	records := collectionlist.NewListWithCapacity[DLQRecord](limit)
	for records.Len() < limit && dlqHasMore(nextOffset, offsets.HighWatermark) {
		next, stop, err := b.scanNextDLQBatch(tp, query, limit, nextOffset, records)
		if err != nil {
			return DLQQueryResult{}, err
		}
		nextOffset = next
		if stop {
			break
		}
	}
	return DLQQueryResult{
		DLQTopic:      tp.Topic,
		Partition:     tp.Partition,
		Offset:        start,
		NextOffset:    nextOffset,
		HasMore:       dlqHasMore(nextOffset, offsets.HighWatermark),
		HighWatermark: offsets.HighWatermark,
		Records:       records.Values(),
	}, nil
}

func (b *Broker) scanNextDLQBatch(
	tp store.TopicPartition,
	query DLQQuery,
	limit int,
	offset uint64,
	records *collectionlist.List[DLQRecord],
) (uint64, bool, error) {
	batch, err := b.queue.ReadFrom(tp, offset, b.cfg.Broker.MaxFetchRecords)
	if err != nil {
		return offset, false, wrapBrokerStore(err, "read dlq records")
	}
	if len(batch) == 0 {
		return offset, true, nil
	}
	next, full, err := appendMatchingDLQRecords(tp, query, limit, batch, records)
	stop := full || len(batch) < b.cfg.Broker.MaxFetchRecords
	return next, stop, err
}

func appendMatchingDLQRecords(
	tp store.TopicPartition,
	query DLQQuery,
	limit int,
	batch []store.Record,
	records *collectionlist.List[DLQRecord],
) (uint64, bool, error) {
	nextOffset := batch[0].Offset
	for index := range batch {
		record := batch[index]
		nextOffset = record.Offset + 1
		dlqRecord, err := dlqRecordFromStore(tp.Topic, tp.Partition, record)
		if err != nil {
			return nextOffset, false, err
		}
		if dlqRecordMatches(query, dlqRecord) {
			records.Add(dlqRecord)
		}
		if records.Len() >= limit {
			return nextOffset, true, nil
		}
	}
	return nextOffset, false, nil
}
