package broker

import (
	"errors"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (r *shardedMessageRuntime) Snapshot() (store.Snapshot, error) {
	topics, err := r.meta.ListTopics()
	if err != nil {
		return store.Snapshot{}, wrapBrokerStore(err, "list sharded snapshot topics")
	}
	out := store.Snapshot{
		Topics:     *collectionlist.NewListWithCapacity[store.TopicConfig](len(topics), topics...),
		Records:    *collectionmapping.NewMap[string, []store.Record](),
		LogOffsets: *collectionmapping.NewMap[string, uint64](),
	}
	var resultErr error
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		if shard == nil {
			return true
		}
		shardSnapshot, err := shard.log.Snapshot()
		if err != nil {
			resultErr = errors.Join(resultErr, wrapBroker("message_shard_snapshot_failed", err, "snapshot message shard %d", shardID))
			return true
		}
		mergeMessageSnapshot(&out, shardSnapshot)
		return true
	})
	return out, resultErr
}

func mergeMessageSnapshot(out *store.Snapshot, shardSnapshot store.Snapshot) {
	shardSnapshot.Records.Range(func(key string, records []store.Record) bool {
		out.Records.Set(key, records)
		return true
	})
	shardSnapshot.LogOffsets.Range(func(key string, offset uint64) bool {
		out.LogOffsets.Set(key, offset)
		return true
	})
}

func (r *shardedMessageRuntime) Restore(snapshot store.Snapshot) error {
	shardSnapshots, err := r.splitRestoreSnapshot(snapshot)
	if err != nil {
		return err
	}
	var resultErr error
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		if shard == nil {
			return true
		}
		shardSnapshot := shardSnapshots.GetOrDefault(shardID, store.Snapshot{
			Topics:     snapshot.Topics,
			Records:    *collectionmapping.NewMap[string, []store.Record](),
			LogOffsets: *collectionmapping.NewMap[string, uint64](),
		})
		err := shard.log.Restore(shardSnapshot)
		resultErr = errors.Join(resultErr, wrapBroker("message_shard_restore_failed", err, "restore message shard %d", shardID))
		return true
	})
	return resultErr
}

func (r *shardedMessageRuntime) splitRestoreSnapshot(snapshot store.Snapshot) (*collectionmapping.Map[store.ShardID, store.Snapshot], error) {
	out := collectionmapping.NewMapWithCapacity[store.ShardID, store.Snapshot](r.shards.Len())
	for _, shardID := range r.shards.Keys() {
		out.Set(shardID, store.Snapshot{
			Topics:     snapshot.Topics,
			Records:    *collectionmapping.NewMap[string, []store.Record](),
			LogOffsets: *collectionmapping.NewMap[string, uint64](),
		})
	}
	if err := splitSnapshotRecords(snapshot, out); err != nil {
		return nil, err
	}
	if err := splitSnapshotLogOffsets(snapshot, out); err != nil {
		return nil, err
	}
	return out, nil
}

func splitSnapshotRecords(snapshot store.Snapshot, out *collectionmapping.Map[store.ShardID, store.Snapshot]) error {
	placements := snapshotPlacementMap(snapshot.Placements)
	var resultErr error
	snapshot.Records.Range(func(key string, records []store.Record) bool {
		shardID, err := snapshotShardID(key, placements)
		if err != nil {
			resultErr = err
			return false
		}
		shardSnapshot, err := snapshotForShard(out, shardID)
		if err != nil {
			resultErr = err
			return false
		}
		shardSnapshot.Records.Set(key, records)
		out.Set(shardID, shardSnapshot)
		return true
	})
	return resultErr
}

func splitSnapshotLogOffsets(snapshot store.Snapshot, out *collectionmapping.Map[store.ShardID, store.Snapshot]) error {
	placements := snapshotPlacementMap(snapshot.Placements)
	var resultErr error
	snapshot.LogOffsets.Range(func(key string, offset uint64) bool {
		shardID, err := snapshotShardID(key, placements)
		if err != nil {
			resultErr = err
			return false
		}
		shardSnapshot, err := snapshotForShard(out, shardID)
		if err != nil {
			resultErr = err
			return false
		}
		shardSnapshot.LogOffsets.Set(key, offset)
		out.Set(shardID, shardSnapshot)
		return true
	})
	return resultErr
}

func snapshotForShard(
	snapshots *collectionmapping.Map[store.ShardID, store.Snapshot],
	shardID store.ShardID,
) (store.Snapshot, error) {
	shardSnapshot, ok := snapshots.Get(shardID)
	if !ok {
		return store.Snapshot{}, brokerStoreError(store.CodeInvalidArgument, "snapshot references unregistered message shard %d", shardID)
	}
	return shardSnapshot, nil
}

func snapshotPlacementMap(
	placements collectionlist.List[store.ShardPlacement],
) *collectionmapping.Map[store.TopicPartition, store.ShardID] {
	out := collectionmapping.NewMapWithCapacity[store.TopicPartition, store.ShardID](placements.Len())
	placements.Range(func(_ int, placement store.ShardPlacement) bool {
		out.Set(placement.TopicPartition(), placement.ShardID)
		return true
	})
	return out
}

func snapshotShardID(
	key string,
	placements *collectionmapping.Map[store.TopicPartition, store.ShardID],
) (store.ShardID, error) {
	tp, err := store.ParseSnapshotPartitionKey(key)
	if err != nil {
		return 0, wrapBroker("snapshot_partition_key_parse_failed", err, "parse snapshot partition key")
	}
	shardID, ok := placements.Get(tp)
	if !ok {
		return 0, brokerStoreError(store.CodeInvalidArgument, "snapshot missing shard placement for %s/%d", tp.Topic, tp.Partition)
	}
	return shardID, nil
}
