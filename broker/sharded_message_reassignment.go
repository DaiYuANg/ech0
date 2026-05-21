package broker

import (
	"sync"

	"github.com/lyonbrown4d/ech0/store"
)

func (r *shardedMessageRuntime) ReassignPartitionData(tp store.TopicPartition, targetShardID store.ShardID, commit func() error) error {
	if commit == nil {
		return brokerStoreError(store.CodeInvalidArgument, "partition reassignment commit function is required")
	}
	unlock := r.lockTopicPartition(tp)
	defer unlock()
	return r.reassignPartitionDataLocked(tp, targetShardID, commit)
}

func (r *shardedMessageRuntime) reassignPartitionDataLocked(tp store.TopicPartition, targetShardID store.ShardID, commit func() error) error {
	sourcePlacement, err := r.resolver.Resolve(tp)
	if err != nil {
		return err
	}
	if sourcePlacement.ShardID == targetShardID {
		return wrapBroker("partition_reassignment_commit_failed", commit(), "commit partition reassignment")
	}
	source, target, err := r.reassignmentShards(sourcePlacement.ShardID, targetShardID)
	if err != nil {
		return err
	}
	if err := movePartitionSnapshot(source, target, tp); err != nil {
		return err
	}
	if err := wrapBroker("partition_reassignment_commit_failed", commit(), "commit partition reassignment"); err != nil {
		return err
	}
	r.clearReassignedSource(source, target, tp)
	return nil
}

func (r *shardedMessageRuntime) reassignmentShards(sourceShardID, targetShardID store.ShardID) (*messageShard, *messageShard, error) {
	source, err := r.shardByID(sourceShardID)
	if err != nil {
		return nil, nil, err
	}
	target, err := r.shardByID(targetShardID)
	if err != nil {
		return nil, nil, err
	}
	return source, target, nil
}

func movePartitionSnapshot(source, target *messageShard, tp store.TopicPartition) error {
	snapshot, err := source.log.SnapshotPartition(tp)
	if err != nil {
		return wrapBrokerStore(err, "snapshot source partition for reassignment")
	}
	if err := target.log.ReplacePartition(snapshot); err != nil {
		return wrapBrokerStore(err, "restore target partition for reassignment")
	}
	return nil
}

func (r *shardedMessageRuntime) clearReassignedSource(source, target *messageShard, tp store.TopicPartition) {
	if err := source.log.ClearPartition(tp); err != nil && r.logger != nil {
		r.logger.Warn("partition reassignment left source shard data behind",
			"topic", tp.Topic,
			"partition", tp.Partition,
			"source_shard", source.id,
			"target_shard", target.id,
			"error", err,
		)
	}
}

func (r *shardedMessageRuntime) shardByID(shardID store.ShardID) (*messageShard, error) {
	shard, ok := r.shards.Get(shardID)
	if !ok || shard == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "message shard %d is not registered", shardID)
	}
	return shard, nil
}

func (r *shardedMessageRuntime) lockTopicPartition(tp store.TopicPartition) func() {
	lock := r.topicPartitionLock(tp)
	lock.Lock()
	return lock.Unlock
}

func (r *shardedMessageRuntime) topicPartitionLock(tp store.TopicPartition) *sync.Mutex {
	r.partitionLocksMu.Lock()
	defer r.partitionLocksMu.Unlock()
	lock, ok := r.partitionLocks.Get(tp)
	if ok {
		return lock
	}
	lock = &sync.Mutex{}
	r.partitionLocks.Set(tp, lock)
	return lock
}
