package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) EnforceRetentionOnce(ctx context.Context) (store.RetentionCleanupResult, error) {
	cleaner, ok := b.queue.(store.RetentionCleaner)
	if !ok {
		return store.RetentionCleanupResult{}, nil
	}
	result, err := cleaner.EnforceRetention(ctx, store.NowMS())
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
	result, err := cleaner.Compact(ctx, store.NowMS(), b.cfg.Storage.CompactionSealedSegmentBatch)
	if err != nil {
		return store.CompactionCleanupResult{}, wrapBroker("compaction_cleanup_failed", err, "compact log records")
	}
	if b.metrics != nil {
		b.metrics.RecordCompactionCleanup(ctx, safeIntToUint64(result.CompactedPartitions), safeIntToUint64(result.RemovedRecords))
	}
	return result, nil
}

func (b *Broker) ExpireTransactionsOnce(ctx context.Context) (TransactionTimeoutCleanupResult, error) {
	return b.ExpireTransactions(ctx, store.NowMS())
}

func (b *Broker) ExpireTransactions(ctx context.Context, nowMS uint64) (TransactionTimeoutCleanupResult, error) {
	req := txExpireCommand{NowMS: nowMS}
	result, err := routeMetadataCommand(ctx, b, raftCommandTxExpire, req, b.applyTxExpire)
	if err != nil {
		return TransactionTimeoutCleanupResult{}, wrapBroker("transaction_cleanup_failed", err, "expire transactions")
	}
	return result, nil
}
