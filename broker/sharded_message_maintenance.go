package broker

import (
	"context"
	"sync"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

type messageShardRef struct {
	id    store.ShardID
	shard *messageShard
}

func (r *shardedMessageRuntime) EnforceRetention(ctx context.Context, nowMS uint64) (store.RetentionCleanupResult, error) {
	result := store.RetentionCleanupResult{}
	refs := r.messageShardRefs()
	var mu sync.Mutex
	err := runBounded(ctx, int64(len(refs)), len(refs), func(workerCtx context.Context, index int) error {
		ref := refs[index]
		if ref.shard == nil {
			return nil
		}
		shardResult, err := ref.shard.log.EnforceRetention(workerCtx, nowMS)
		if err != nil {
			return wrapBroker("message_shard_retention_failed", err, "enforce retention on message shard %d", ref.id)
		}
		mu.Lock()
		result.RemovedRecords += shardResult.RemovedRecords
		mu.Unlock()
		return nil
	})
	return result, err
}

func (r *shardedMessageRuntime) messageShardRefs() []messageShardRef {
	if r == nil || r.shards == nil {
		return nil
	}
	refs := collectionlist.NewListWithCapacity[messageShardRef](r.shards.Len())
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		refs.Add(messageShardRef{id: shardID, shard: shard})
		return true
	})
	return refs.Values()
}

func (r *shardedMessageRuntime) Compact(ctx context.Context, nowMS uint64, sealedSegmentBatch int) (store.CompactionCleanupResult, error) {
	result := store.CompactionCleanupResult{}
	refs := r.messageShardRefs()
	var mu sync.Mutex
	err := runBounded(ctx, int64(len(refs)), len(refs), func(workerCtx context.Context, index int) error {
		ref := refs[index]
		if ref.shard == nil {
			return nil
		}
		shardResult, err := ref.shard.log.Compact(workerCtx, nowMS, sealedSegmentBatch)
		if err != nil {
			return wrapBroker("message_shard_compaction_failed", err, "compact message shard %d", ref.id)
		}
		mu.Lock()
		result.CompactedPartitions += shardResult.CompactedPartitions
		result.RemovedRecords += shardResult.RemovedRecords
		mu.Unlock()
		return nil
	})
	return result, err
}
