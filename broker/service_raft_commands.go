package broker

import (
	"context"
	"encoding/json"

	"github.com/DaiYuANg/ech0/store"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func (b *Broker) applyRaftCommand(data []byte) (any, error) {
	var cmd raftCommand
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, wrapBroker("raft_command_decode_failed", err, "decode raft command")
	}
	handler, ok := b.raftCommandHandlers().Get(cmd.Type)
	if !ok {
		return nil, brokerStoreError(store.CodeCodec, "unknown raft command %s", cmd.Type)
	}
	return handler(context.Background(), cmd.Payload)
}

type raftCommandHandler func(context.Context, json.RawMessage) (any, error)

func setRaftHandler[R any, T any](
	handlers *collectionmapping.Map[string, raftCommandHandler],
	commandType string,
	apply func(context.Context, R) (T, error),
	label string,
) {
	handlers.Set(commandType, func(ctx context.Context, payload json.RawMessage) (any, error) {
		return decodeRaftCommand(ctx, payload, apply, label)
	})
}

func (b *Broker) raftCommandHandlers() *collectionmapping.Map[string, raftCommandHandler] {
	handlers := collectionmapping.NewMap[string, raftCommandHandler]()
	b.registerTopicRaftHandlers(handlers)
	b.registerDirectRaftHandlers(handlers)
	b.registerGroupRaftHandlers(handlers)
	b.registerRetryDelayRaftHandlers(handlers)
	return handlers
}

func (b *Broker) registerTopicRaftHandlers(handlers *collectionmapping.Map[string, raftCommandHandler]) {
	setRaftHandler(handlers, raftCommandCreateTopic, b.applyCreateTopic, "decode create topic command")
	setRaftHandler(handlers, raftCommandProduce, b.applyProduce, "decode produce command")
	setRaftHandler(handlers, raftCommandProduceBatch, b.applyProduceBatch, "decode produce batch command")
	setRaftHandler(handlers, raftCommandCommitOffset, b.applyCommitOffset, "decode commit offset command")
}

func (b *Broker) registerDirectRaftHandlers(handlers *collectionmapping.Map[string, raftCommandHandler]) {
	setRaftHandler(handlers, raftCommandDirectSend, b.applyDirectSend, "decode direct send command")
	setRaftHandler(handlers, raftCommandDirectAck, b.applyDirectAck, "decode direct ack command")
}

func (b *Broker) registerGroupRaftHandlers(handlers *collectionmapping.Map[string, raftCommandHandler]) {
	setRaftHandler(handlers, raftCommandJoinGroup, b.applyJoinGroup, "decode join group command")
	setRaftHandler(handlers, raftCommandHeartbeatGroup, b.applyHeartbeatGroup, "decode heartbeat group command")
	setRaftHandler(handlers, raftCommandRebalanceGroup, b.applyRebalanceGroup, "decode rebalance group command")
}

func (b *Broker) registerRetryDelayRaftHandlers(handlers *collectionmapping.Map[string, raftCommandHandler]) {
	setRaftHandler(handlers, raftCommandNack, b.applyNack, "decode nack command")
	setRaftHandler(handlers, raftCommandProcessRetry, b.applyProcessRetryBatch, "decode process retry command")
	setRaftHandler(handlers, raftCommandScheduleDelay, b.applyScheduleDelay, "decode schedule delay command")
	setRaftHandler(handlers, raftCommandProcessDelay, b.applyProcessDelayPartition, "decode process delay command")
}

func decodeRaftCommand[R any, T any](ctx context.Context, payload json.RawMessage, apply func(context.Context, R) (T, error), label string) (any, error) {
	var req R
	if err := json.Unmarshal(payload, &req); err != nil {
		return nil, wrapBroker("raft_payload_decode_failed", err, "%s", label)
	}
	return apply(ctx, req)
}

func proposeOrApply[T any, R any](ctx context.Context, b *Broker, commandType string, req R, apply func(context.Context, R) (T, error)) (T, error) {
	b.raftMu.RLock()
	node := b.raft
	b.raftMu.RUnlock()
	if !b.cfg.Raft.Enabled || node == nil {
		return apply(ctx, req)
	}
	var zero T
	value, err := node.Apply(ctx, commandType, req)
	if err != nil {
		return zero, err
	}
	typed, ok := value.(T)
	if ok {
		return typed, nil
	}
	marshaled, err := json.Marshal(value)
	if err != nil {
		return zero, wrapBroker("raft_result_encode_failed", err, "encode raft result")
	}
	if err := json.Unmarshal(marshaled, &typed); err != nil {
		return zero, wrapBroker("raft_result_decode_failed", err, "decode raft result")
	}
	return typed, nil
}
