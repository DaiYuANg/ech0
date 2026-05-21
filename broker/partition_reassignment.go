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
	_, err := routePartitionCommand(ctx, b, exactPartitionCommandTarget(scopedTopic, partition), raftCommandReassignPartition, req, b.applyReassignPartition)
	return err
}

type partitionDataReassignmentRuntime interface {
	ReassignPartitionData(store.TopicPartition, store.ShardID, func() error) error
}

func (b *Broker) applyReassignPartition(_ context.Context, req reassignPartitionCommand) (store.ShardPlacement, error) {
	if err := b.validatePartitionReassignment(req.Topic, req.Partition, req.ShardID); err != nil {
		return store.ShardPlacement{}, err
	}
	if mover, ok := b.queue.(partitionDataReassignmentRuntime); ok {
		var placement store.ShardPlacement
		err := mover.ReassignPartitionData(store.NewTopicPartition(req.Topic, req.Partition), req.ShardID, func() error {
			saved, saveErr := b.saveReassignedPartitionPlacement(req)
			placement = saved
			return saveErr
		})
		if err != nil {
			return store.ShardPlacement{}, wrapBroker("partition_reassignment_failed", err, "reassign partition data")
		}
		return placement, nil
	}
	return b.saveReassignedPartitionPlacement(req)
}

func (b *Broker) saveReassignedPartitionPlacement(req reassignPartitionCommand) (store.ShardPlacement, error) {
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
	return nil
}
