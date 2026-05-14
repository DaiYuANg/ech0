package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/store"
)

type partitionCommandTarget struct {
	Topic          string
	Partition      uint32
	PartitionKnown bool
	ShardID        store.ShardID
	ShardKnown     bool
}

type commandApplyFunc func(context.Context) (any, error)

type metadataCommandRouter interface {
	ApplyMetadataCommand(context.Context, string, any, commandApplyFunc) (any, error)
}

type partitionCommandRouter interface {
	ApplyPartitionCommand(context.Context, partitionCommandTarget, string, any, commandApplyFunc) (any, error)
}

type brokerCommandRouter interface {
	metadataCommandRouter
	partitionCommandRouter
	UsesCluster() bool
}

type singleGroupCommandRouter struct {
	broker *Broker
}

func newSingleGroupCommandRouter(b *Broker) brokerCommandRouter {
	return singleGroupCommandRouter{broker: b}
}

type metadataOnlyCommandRouter struct {
	metadata brokerCommandRouter
}

func newMetadataOnlyCommandRouter(metadata brokerCommandRouter) brokerCommandRouter {
	return metadataOnlyCommandRouter{metadata: metadata}
}

func topicCommandTarget(topic string) partitionCommandTarget {
	return partitionCommandTarget{Topic: topic}
}

func exactPartitionCommandTarget(topic string, partition uint32) partitionCommandTarget {
	return partitionCommandTarget{Topic: topic, Partition: partition, PartitionKnown: true}
}

func shardCommandTarget(shardID store.ShardID) partitionCommandTarget {
	return partitionCommandTarget{ShardID: shardID, ShardKnown: true}
}

func publishPartitionCommandTarget(topic string, partitioning PublishPartitioning) partitionCommandTarget {
	if partitioning.Mode == PartitionExplicit {
		return exactPartitionCommandTarget(topic, partitioning.Partition)
	}
	return topicCommandTarget(topic)
}

func (target partitionCommandTarget) validate() error {
	if target.PartitionKnown && target.Topic == "" {
		return brokerStoreError(store.CodeInvalidArgument, "partition command target missing topic for partition %d", target.Partition)
	}
	return nil
}

func (r singleGroupCommandRouter) ApplyMetadataCommand(
	ctx context.Context,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	return r.apply(ctx, commandType, payload, local)
}

func (r singleGroupCommandRouter) ApplyPartitionCommand(
	ctx context.Context,
	target partitionCommandTarget,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	if err := target.validate(); err != nil {
		return nil, err
	}
	return r.apply(ctx, commandType, payload, local)
}

func (r singleGroupCommandRouter) UsesCluster() bool {
	return r.raftNode() != nil
}

func (r singleGroupCommandRouter) apply(ctx context.Context, commandType string, payload any, local commandApplyFunc) (any, error) {
	node := r.raftNode()
	if node == nil {
		return local(ctx)
	}
	return node.Apply(ctx, commandType, payload)
}

func (r singleGroupCommandRouter) raftNode() *raftNode {
	if r.broker == nil {
		return nil
	}
	return r.broker.currentRaftNode()
}

func (r metadataOnlyCommandRouter) ApplyMetadataCommand(
	ctx context.Context,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	value, err := r.metadata.ApplyMetadataCommand(ctx, commandType, payload, local)
	if err != nil {
		return nil, wrapBroker("metadata_only_command_route_failed", err, "route metadata command %s", commandType)
	}
	return value, nil
}

func (r metadataOnlyCommandRouter) ApplyPartitionCommand(
	ctx context.Context,
	target partitionCommandTarget,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	_ = commandType
	_ = payload
	if err := target.validate(); err != nil {
		return nil, err
	}
	return local(ctx)
}

func (r metadataOnlyCommandRouter) UsesCluster() bool {
	return false
}

func (b *Broker) commandRouter() brokerCommandRouter {
	if b == nil || b.commands == nil {
		return newSingleGroupCommandRouter(b)
	}
	return b.commands
}

func (b *Broker) usesClusterCommandRouter() bool {
	return b != nil && b.commandRouter().UsesCluster()
}

func routeMetadataCommand[T any, R any](
	ctx context.Context,
	b *Broker,
	commandType string,
	req R,
	apply func(context.Context, R) (T, error),
) (T, error) {
	return routeCommand(b, req, apply, func(router brokerCommandRouter, local commandApplyFunc) (any, error) {
		return router.ApplyMetadataCommand(ctx, commandType, req, local)
	})
}

func routePartitionCommand[T any, R any](
	ctx context.Context,
	b *Broker,
	target partitionCommandTarget,
	commandType string,
	req R,
	apply func(context.Context, R) (T, error),
) (T, error) {
	return routeCommand(b, req, apply, func(router brokerCommandRouter, local commandApplyFunc) (any, error) {
		return router.ApplyPartitionCommand(ctx, target, commandType, req, local)
	})
}

func routeCommand[T any, R any](
	b *Broker,
	req R,
	apply func(context.Context, R) (T, error),
	dispatch func(brokerCommandRouter, commandApplyFunc) (any, error),
) (T, error) {
	local := func(ctx context.Context) (any, error) {
		return apply(ctx, req)
	}
	var zero T
	value, err := dispatch(b.commandRouter(), local)
	if err != nil {
		return zero, err
	}
	return raftValueAs[T](value)
}

func (b *Broker) routeCoalescedPartitionCommand(ctx context.Context, commandType string, payload any) (any, error) {
	switch commandType {
	case raftCommandProduceBatches:
		req, ok := payload.(produceBatchesCommand)
		if !ok {
			return nil, brokerStoreError(store.CodeCodec, "invalid coalesced produce payload %T", payload)
		}
		return b.routeProduceBatchesCommand(ctx, req)
	case raftCommandCommitOffsets:
		req, ok := payload.(commitOffsetsCommand)
		if !ok {
			return nil, brokerStoreError(store.CodeCodec, "invalid coalesced commit payload %T", payload)
		}
		return b.routeCommitOffsetsCommand(ctx, req)
	default:
		return nil, brokerStoreError(store.CodeCodec, "unsupported coalesced partition command %s", commandType)
	}
}

func (b *Broker) applyRoutedPartitionCommand(
	ctx context.Context,
	target partitionCommandTarget,
	commandType string,
	payload any,
	local commandApplyFunc,
) (any, error) {
	value, err := b.commandRouter().ApplyPartitionCommand(ctx, target, commandType, payload, local)
	if err != nil {
		return nil, wrapBroker("partition_command_route_failed", err, "route coalesced partition command %s", commandType)
	}
	return value, nil
}

func produceBatchesCommandTarget(req produceBatchesCommand) partitionCommandTarget {
	if len(req.Requests) != 1 {
		return partitionCommandTarget{}
	}
	item := req.Requests[0]
	return publishPartitionCommandTarget(item.Topic, item.Partitioning)
}

func commitOffsetsCommandTarget(req commitOffsetsCommand) partitionCommandTarget {
	if len(req.Requests) != 1 {
		return partitionCommandTarget{}
	}
	item := req.Requests[0]
	return exactPartitionCommandTarget(item.Topic, item.Partition)
}
