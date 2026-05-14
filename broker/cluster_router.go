package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/store"
)

type clusterCommandRouter struct {
	metadata brokerCommandRouter
	data     dataShardRuntime
	shards   *brokerShardResolver
}

func newClusterCommandRouter(metadata brokerCommandRouter, data dataShardRuntime, shards *brokerShardResolver) brokerCommandRouter {
	return clusterCommandRouter{metadata: metadata, data: data, shards: shards}
}

func (r clusterCommandRouter) ApplyMetadataCommand(
	ctx context.Context,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	value, err := r.metadata.ApplyMetadataCommand(ctx, commandType, payload, local)
	if err != nil {
		return nil, wrapBroker("metadata_command_route_failed", err, "route metadata command %s", commandType)
	}
	return value, nil
}

func (r clusterCommandRouter) ApplyPartitionCommand(
	ctx context.Context,
	target partitionCommandTarget,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	resolved, err := r.resolveTarget(target)
	if err != nil {
		return nil, wrapBroker("partition_command_target_resolve_failed", err, "resolve partition command target %s", commandType)
	}
	value, err := r.applyResolvedPartitionCommand(ctx, resolved, commandType, payload, local)
	if err != nil {
		return nil, wrapBroker("partition_command_route_failed", err, "route partition command %s", commandType)
	}
	return value, nil
}

func (r clusterCommandRouter) UsesCluster() bool {
	return r.metadata.UsesCluster()
}

func (r clusterCommandRouter) applyResolvedPartitionCommand(
	ctx context.Context,
	target partitionCommandTarget,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	if target.ShardKnown && r.data != nil {
		value, err := r.data.ApplyDataShardCommand(ctx, dataShardCommand{
			ShardID:     target.ShardID,
			Target:      target,
			CommandType: commandType,
			Payload:     payload,
			Local:       local,
		})
		if err != nil {
			return nil, wrapBroker("data_shard_command_apply_failed", err, "apply data shard command %s to shard %d", commandType, target.ShardID)
		}
		return value, nil
	}
	value, err := r.metadata.ApplyPartitionCommand(ctx, target, commandType, payload, local)
	if err != nil {
		return nil, wrapBroker("partition_command_metadata_fallback_failed", err, "apply partition command %s through metadata fallback", commandType)
	}
	return value, nil
}

func (r clusterCommandRouter) resolveTarget(target partitionCommandTarget) (partitionCommandTarget, error) {
	if err := target.validate(); err != nil {
		return partitionCommandTarget{}, err
	}
	if target.ShardKnown || !target.PartitionKnown || r.shards == nil {
		return target, nil
	}
	placement, err := r.shards.Resolve(store.NewTopicPartition(target.Topic, target.Partition))
	if err != nil {
		return partitionCommandTarget{}, err
	}
	target.ShardID = placement.ShardID
	target.ShardKnown = true
	return target, nil
}
