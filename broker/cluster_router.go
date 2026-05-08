package broker

import (
	"context"

	"github.com/DaiYuANg/ech0/store"
)

type clusterCommandRouter struct {
	fallback brokerCommandRouter
	shards   *brokerShardResolver
}

func newClusterCommandRouter(fallback brokerCommandRouter, shards *brokerShardResolver) brokerCommandRouter {
	return clusterCommandRouter{fallback: fallback, shards: shards}
}

func (r clusterCommandRouter) ApplyMetadataCommand(
	ctx context.Context,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	value, err := r.fallback.ApplyMetadataCommand(ctx, commandType, payload, local)
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
	value, err := r.fallback.ApplyPartitionCommand(ctx, resolved, commandType, payload, local)
	if err != nil {
		return nil, wrapBroker("partition_command_route_failed", err, "route partition command %s", commandType)
	}
	return value, nil
}

func (r clusterCommandRouter) UsesCluster() bool {
	return r.fallback.UsesCluster()
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
