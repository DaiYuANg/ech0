package broker

import (
	"context"

	"github.com/DaiYuANg/ech0/store"
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
