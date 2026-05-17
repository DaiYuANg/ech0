package broker

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) queryDLQByIndex(
	tp store.TopicPartition,
	scopedSource string,
	query DLQQuery,
	limit int,
	offsets store.PartitionOffsetState,
) (DLQQueryResult, bool, error) {
	start := max(query.Offset, offsets.LogStartOffset)
	indexes, err := b.meta.ListDLQIndexes(dlqIndexFilter(tp.Topic, scopedSource, query, start))
	if err != nil {
		return DLQQueryResult{}, false, wrapBrokerStore(err, "query dlq indexes")
	}
	if len(indexes) == 0 {
		return DLQQueryResult{}, false, nil
	}
	records := collectionlist.NewListWithCapacity[DLQRecord](limit)
	nextOffset, err := b.appendIndexedDLQRecords(tp, query, limit, indexes, records)
	if err != nil {
		return DLQQueryResult{}, false, err
	}
	return DLQQueryResult{
		DLQTopic:      tp.Topic,
		Partition:     tp.Partition,
		Offset:        start,
		NextOffset:    nextOffset,
		HasMore:       dlqHasMore(nextOffset, offsets.HighWatermark),
		HighWatermark: offsets.HighWatermark,
		Records:       records.Values(),
	}, true, nil
}

func (b *Broker) appendIndexedDLQRecords(
	tp store.TopicPartition,
	query DLQQuery,
	limit int,
	indexes []store.DLQIndexEntry,
	records *collectionlist.List[DLQRecord],
) (uint64, error) {
	nextOffset := query.Offset
	for index := range indexes {
		entry := indexes[index]
		nextOffset = entry.DLQOffset + 1
		record, err := b.readRecordAtOffset(tp, entry.DLQOffset)
		if err != nil {
			return nextOffset, err
		}
		dlqRecord, err := dlqRecordFromStore(tp.Topic, tp.Partition, record)
		if err != nil {
			return nextOffset, err
		}
		if dlqRecordMatches(query, dlqRecord) {
			records.Add(dlqRecord)
		}
		if records.Len() >= limit {
			return nextOffset, nil
		}
	}
	return nextOffset, nil
}
