package broker

import (
	"context"

	"github.com/DaiYuANg/ech0/store"
)

func (b *Broker) EnforceRetentionOnce(ctx context.Context) (store.RetentionCleanupResult, error) {
	cleaner, ok := b.queue.(store.RetentionCleaner)
	if !ok {
		return store.RetentionCleanupResult{}, nil
	}
	result, err := cleaner.EnforceRetention(store.NowMS())
	if err != nil {
		return store.RetentionCleanupResult{}, wrapBroker("retention_cleanup_failed", err, "enforce retention cleanup")
	}
	if b.metrics != nil {
		b.metrics.RecordRetentionCleanup(ctx, safeIntToUint64(result.RemovedRecords))
	}
	return result, nil
}

func (b *Broker) CompactOnce(ctx context.Context) (store.CompactionCleanupResult, error) {
	cleaner, ok := b.queue.(store.CompactionCleaner)
	if !ok {
		return store.CompactionCleanupResult{}, nil
	}
	result, err := cleaner.Compact(store.NowMS(), b.cfg.Storage.CompactionSealedSegmentBatch)
	if err != nil {
		return store.CompactionCleanupResult{}, wrapBroker("compaction_cleanup_failed", err, "compact log records")
	}
	if b.metrics != nil {
		b.metrics.RecordCompactionCleanup(ctx, safeIntToUint64(result.CompactedPartitions), safeIntToUint64(result.RemovedRecords))
	}
	return result, nil
}
