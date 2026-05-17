package broker

import "github.com/lyonbrown4d/ech0/store"

func (b *Broker) saveDLQIndexFromRecord(dlqTopic string, partition uint32, record store.Record) error {
	entry, err := dlqIndexEntryFromStore(dlqTopic, partition, record)
	if err != nil {
		return err
	}
	return wrapBrokerStore(b.meta.SaveDLQIndex(entry), "save dlq index")
}

func dlqIndexEntryFromStore(dlqTopic string, partition uint32, record store.Record) (store.DLQIndexEntry, error) {
	dlqRecord, err := dlqRecordFromStore(dlqTopic, partition, record)
	if err != nil {
		return store.DLQIndexEntry{}, err
	}
	return store.DLQIndexEntry{
		DLQTopic:          dlqTopic,
		DLQPartition:      partition,
		DLQOffset:         record.Offset,
		TimestampMS:       record.TimestampMS,
		OriginalTopic:     dlqRecord.OriginalTopic,
		OriginalPartition: dlqRecord.OriginalPartition,
		OriginalOffset:    dlqRecord.OriginalOffset,
		ErrorCode:         dlqRecord.ErrorCode,
		RetryCount:        dlqRecord.RetryCount,
	}, nil
}

func dlqIndexFilter(dlqTopic, scopedSource string, query DLQQuery, start uint64) store.DLQIndexFilter {
	partition := query.Partition
	return store.DLQIndexFilter{
		DLQTopic:          dlqTopic,
		DLQPartition:      &partition,
		MinDLQOffset:      &start,
		FromTimestampMS:   query.FromTimestampMS,
		ToTimestampMS:     query.ToTimestampMS,
		OriginalTopic:     &scopedSource,
		OriginalPartition: query.OriginalPartition,
		OriginalOffset:    query.OriginalOffset,
		ErrorCode:         query.ErrorCode,
		RetryCount:        query.RetryCount,
	}
}
