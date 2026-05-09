package broker

import (
	"context"
	"errors"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
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
	EnsureTopic(context.Context, store.TopicConfig) error
	Close() error
	RuntimeMode() string
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

func (r dataShardRegistry) EnsureTopic(ctx context.Context, topic store.TopicConfig) error {
	var result error
	r.runtimes.Range(func(shardID store.ShardID, runtime dataShardRuntime) bool {
		if runtime != nil {
			err := runtime.EnsureTopic(ctx, topic)
			result = errors.Join(result, wrapBroker("data_shard_topic_init_failed", err, "ensure topic %s on data shard %d", topic.Name, shardID))
		}
		return true
	})
	return result
}

func (r dataShardRegistry) Close() error {
	var result error
	r.runtimes.Range(func(shardID store.ShardID, runtime dataShardRuntime) bool {
		if runtime != nil {
			err := runtime.Close()
			result = errors.Join(result, wrapBroker("data_shard_close_failed", err, "close data shard %d", shardID))
		}
		return true
	})
	return result
}

func (r dataShardRegistry) RuntimeMode() string {
	return "registry"
}

func (r dataShardRegistry) RuntimeModeForShard(shardID store.ShardID) string {
	runtime, ok := r.runtimes.Get(shardID)
	if !ok || runtime == nil {
		return "missing"
	}
	return runtime.RuntimeMode()
}

func dataShardHealth(specs []dataShardSpec, runtime dataShardRuntime) []DataShardHealth {
	out := collectionlist.NewListWithCapacity[DataShardHealth](len(specs))
	for _, spec := range specs {
		out.Add(DataShardHealth{ShardID: spec.ShardID, RuntimeMode: dataShardRuntimeMode(runtime, spec.ShardID)})
	}
	return out.Values()
}

func dataShardRuntimeMode(runtime dataShardRuntime, shardID store.ShardID) string {
	if runtime == nil {
		return "missing"
	}
	if registry, ok := runtime.(interface{ RuntimeModeForShard(store.ShardID) string }); ok {
		return registry.RuntimeModeForShard(shardID)
	}
	return runtime.RuntimeMode()
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
		return nil, wrapBroker("single_group_data_shard_apply_failed", err, "apply data shard command %s to standalone router", cmd.CommandType)
	}
	return value, nil
}

func (r singleGroupDataShardRuntime) EnsureTopic(context.Context, store.TopicConfig) error {
	return nil
}

func (r singleGroupDataShardRuntime) Close() error {
	return nil
}

func (r singleGroupDataShardRuntime) RuntimeMode() string {
	return "standalone"
}

type raftDataShardRuntime struct {
	broker  *Broker
	shardID store.ShardID
}

func newRaftDataShardRegistry(b *Broker, specs []dataShardSpec) dataShardRuntime {
	if len(specs) == 0 {
		specs = []dataShardSpec{{ShardID: 0}}
	}
	runtimes := collectionmapping.NewMapWithCapacity[store.ShardID, dataShardRuntime](len(specs))
	for _, spec := range specs {
		runtimes.Set(spec.ShardID, raftDataShardRuntime{broker: b, shardID: spec.ShardID})
	}
	return dataShardRegistry{runtimes: runtimes}
}

func (r raftDataShardRuntime) ApplyDataShardCommand(ctx context.Context, cmd dataShardCommand) (any, error) {
	node := r.broker.currentRaftNode()
	if node == nil {
		return nil, brokerStoreError(store.CodeUnavailable, "raft data shard runtime is not started")
	}
	return node.ApplyGroup(ctx, dataShardRaftGroupID(r.shardID), cmd.CommandType, cmd.Payload)
}

func (r raftDataShardRuntime) EnsureTopic(context.Context, store.TopicConfig) error {
	return nil
}

func (r raftDataShardRuntime) Close() error {
	return nil
}

func (r raftDataShardRuntime) RuntimeMode() string {
	return "dragonboat_group"
}

func (b *Broker) ensureTopicDataShards(ctx context.Context, topic store.TopicConfig) error {
	if b == nil || b.dataShards == nil {
		return nil
	}
	return wrapBroker("data_shards_topic_init_failed", b.dataShards.EnsureTopic(ctx, topic), "ensure topic %s on data shards", topic.Name)
}

func (b *Broker) closeDataShards() error {
	if b == nil || b.dataShards == nil {
		return nil
	}
	return wrapBroker("data_shards_close_failed", b.dataShards.Close(), "close data shards")
}
