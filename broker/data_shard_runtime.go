package broker

import (
	"context"

	"github.com/DaiYuANg/ech0/store"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

type dataShardCommand struct {
	ShardID     store.ShardID
	Target      partitionCommandTarget
	CommandType string
	Payload     any
	Local       commandApplyFunc
}

type dataShardRuntime interface {
	ApplyDataShardCommand(context.Context, dataShardCommand) (any, error)
}

type dataShardRegistry struct {
	runtimes *collectionmapping.Map[store.ShardID, dataShardRuntime]
}

func newCompatibilityDataShardRegistry(specs []dataShardSpec, runtime dataShardRuntime) dataShardRuntime {
	if len(specs) == 0 {
		specs = []dataShardSpec{{ShardID: 0}}
	}
	runtimes := collectionmapping.NewMapWithCapacity[store.ShardID, dataShardRuntime](len(specs))
	for _, spec := range specs {
		runtimes.Set(spec.ShardID, runtime)
	}
	return dataShardRegistry{runtimes: runtimes}
}

func (r dataShardRegistry) ApplyDataShardCommand(ctx context.Context, cmd dataShardCommand) (any, error) {
	runtime, ok := r.runtimes.Get(cmd.ShardID)
	if !ok || runtime == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "data shard %d is not registered", cmd.ShardID)
	}
	value, err := runtime.ApplyDataShardCommand(ctx, cmd)
	if err != nil {
		return nil, wrapBroker("data_shard_runtime_apply_failed", err, "apply data shard command %s on shard %d", cmd.CommandType, cmd.ShardID)
	}
	return value, nil
}

type singleGroupDataShardRuntime struct {
	router brokerCommandRouter
}

func newSingleGroupDataShardRuntime(router brokerCommandRouter) dataShardRuntime {
	return singleGroupDataShardRuntime{router: router}
}

func (r singleGroupDataShardRuntime) ApplyDataShardCommand(ctx context.Context, cmd dataShardCommand) (any, error) {
	target := cmd.Target
	target.ShardID = cmd.ShardID
	target.ShardKnown = true
	value, err := r.router.ApplyPartitionCommand(ctx, target, cmd.CommandType, cmd.Payload, cmd.Local)
	if err != nil {
		return nil, wrapBroker("single_group_data_shard_apply_failed", err, "apply data shard command %s to compatibility raft group", cmd.CommandType)
	}
	return value, nil
}
