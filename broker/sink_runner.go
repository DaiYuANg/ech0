package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/store"
)

type SinkResult struct {
	Delivered           int
	CommittedNextOffset *uint64
}

type recordSinkRun struct {
	Identity   Identity
	Consumer   string
	Topic      string
	Partition  uint32
	MaxRecords int
	Metadata   string
}

func (b *Broker) processRecordSinkOnce(
	ctx context.Context,
	run recordSinkRun,
	deliver func(context.Context, store.Record) error,
) (SinkResult, error) {
	runCtx := WithIdentity(ctx, run.Identity)
	poll, err := b.FetchWithIsolation(runCtx, run.Consumer, run.Topic, run.Partition, nil, run.MaxRecords, FetchIsolationReadCommitted)
	if err != nil {
		return SinkResult{}, err
	}
	result := SinkResult{}
	for index := range poll.Records {
		record := poll.Records[index]
		if err := deliver(ctx, record); err != nil {
			return result, err
		}
		nextOffset := record.Offset + 1
		if err := b.CommitOffsetWithMetadata(runCtx, run.Consumer, run.Topic, run.Partition, nextOffset, run.Metadata); err != nil {
			return result, err
		}
		result.Delivered++
		result.CommittedNextOffset = &nextOffset
	}
	return result, nil
}
