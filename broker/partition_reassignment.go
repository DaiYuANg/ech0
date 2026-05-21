package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) ReassignPartition(ctx context.Context, topic string, partition uint32, targetShardID store.ShardID) error {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAlter, topicResource(identity, topic)); err != nil {
		return err
	}
	scopedTopic := scopedTopicName(identity, topic)
	if err := b.validatePartitionReassignment(scopedTopic, partition, targetShardID); err != nil {
		return err
	}
	req := reassignPartitionCommand{Topic: scopedTopic, Partition: partition, ShardID: targetShardID}
	_, err := routeMetadataCommand(ctx, b, raftCommandReassignPartition, req, b.applyReassignPartition)
	return err
}

func (b *Broker) applyReassignPartition(_ context.Context, req reassignPartitionCommand) (store.ShardPlacement, error) {
	if err := b.validatePartitionReassignment(req.Topic, req.Partition, req.ShardID); err != nil {
		return store.ShardPlacement{}, err
	}
	placement := store.NewShardPlacement(req.Topic, req.Partition, req.ShardID)
	placements, ok := b.meta.(store.ShardPlacementStore)
	if !ok {
		return store.ShardPlacement{}, brokerStoreError(store.CodeInvalidArgument, "metadata store does not support shard placements")
	}
	if err := placements.SaveShardPlacement(placement); err != nil {
		return store.ShardPlacement{}, wrapBrokerStore(err, "save reassigned shard placement")
	}
	if b.shards != nil {
		b.shards.cacheShardPlacement(placement)
	}
	return placement, nil
}

func (b *Broker) validatePartitionReassignment(topic string, partition uint32, targetShardID store.ShardID) error {
	if b == nil || b.shards == nil {
		return brokerStoreError(store.CodeInvalidArgument, "shard resolver is not configured")
	}
	if uint32(targetShardID) >= b.shards.shardCount {
		return brokerStoreError(store.CodeInvalidArgument, "target shard %d is outside configured shard count %d", targetShardID, b.shards.shardCount)
	}
	topicConfig, err := b.loadTopicConfig(topic)
	if err != nil {
		return err
	}
	if topicConfig == nil {
		return brokerStoreError(store.CodeTopicNotFound, "topic %s not found", topic)
	}
	if partition >= topicConfig.Partitions {
		return brokerStoreError(store.CodeInvalidArgument, "partition %d is outside topic %s partition count %d", partition, topic, topicConfig.Partitions)
	}
	offsets, err := b.queue.PartitionOffsets(store.NewTopicPartition(topic, partition))
	if err != nil {
		return wrapBrokerStore(err, "load partition offsets before reassignment")
	}
	if offsets.RetainedRecords > 0 || offsets.NextOffset > offsets.LogStartOffset {
		return brokerStoreError(store.CodeInvalidArgument, "live partition reassignment requires data movement and is not supported yet")
	}
	return nil
}
