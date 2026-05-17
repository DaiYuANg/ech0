package broker

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

type DLQBulkReplayResult struct {
	DLQTopic   string
	Partition  uint32
	Replayed   []DLQReplayResult
	NextOffset uint64
	HasMore    bool
}

func (b *Broker) ReplayDLQQuery(ctx context.Context, query DLQQuery) (DLQBulkReplayResult, error) {
	result, err := b.QueryDLQ(ctx, query)
	if err != nil {
		return DLQBulkReplayResult{}, err
	}
	replayed := collectionlist.NewListWithCapacity[DLQReplayResult](len(result.Records))
	for index := range result.Records {
		record := result.Records[index]
		item, replayErr := b.ReplayDLQ(ctx, query.SourceTopic, record.DLQPartition, record.DLQOffset)
		if replayErr != nil {
			return DLQBulkReplayResult{}, replayErr
		}
		replayed.Add(item)
	}
	return DLQBulkReplayResult{
		DLQTopic:   result.DLQTopic,
		Partition:  result.Partition,
		Replayed:   replayed.Values(),
		NextOffset: result.NextOffset,
		HasMore:    result.HasMore,
	}, nil
}
