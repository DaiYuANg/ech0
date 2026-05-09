package broker

import (
	"errors"

	"github.com/DaiYuANg/ech0/store"
)

func (r *shardedMessageRuntime) EnforceRetention(nowMS uint64) (store.RetentionCleanupResult, error) {
	result := store.RetentionCleanupResult{}
	var resultErr error
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		if shard == nil {
			return true
		}
		shardResult, err := shard.log.EnforceRetention(nowMS)
		if err != nil {
			resultErr = errors.Join(resultErr, wrapBroker("message_shard_retention_failed", err, "enforce retention on message shard %d", shardID))
			return true
		}
		result.RemovedRecords += shardResult.RemovedRecords
		return true
	})
	return result, resultErr
}

func (r *shardedMessageRuntime) Compact(nowMS uint64, sealedSegmentBatch int) (store.CompactionCleanupResult, error) {
	result := store.CompactionCleanupResult{}
	var resultErr error
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		if shard == nil {
			return true
		}
		shardResult, err := shard.log.Compact(nowMS, sealedSegmentBatch)
		if err != nil {
			resultErr = errors.Join(resultErr, wrapBroker("message_shard_compaction_failed", err, "compact message shard %d", shardID))
			return true
		}
		result.CompactedPartitions += shardResult.CompactedPartitions
		result.RemovedRecords += shardResult.RemovedRecords
		return true
	})
	return result, resultErr
}
