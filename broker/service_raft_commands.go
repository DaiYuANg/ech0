package broker

import (
	"context"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) applyRaftCommand(data []byte) (value any, err error) {
	ctx := context.Background()
	commandType := "unknown"
	totalStart := time.Now()
	defer func() {
		b.recordFSMStage(ctx, commandType, "total", totalStart, err)
	}()

	decodeStart := time.Now()
	binaryCommandType, binaryBody, binaryCommand, binaryErr := decodeBinaryRaftEnvelope(data)
	if binaryCommand {
		if binaryErr != nil {
			err = wrapBroker("raft_command_decode_failed", binaryErr, "decode binary raft command")
			b.recordFSMStage(ctx, commandType, "decode_envelope", decodeStart, err)
			return nil, err
		}
		commandType = binaryCommandType
		b.recordFSMStage(ctx, commandType, "decode_envelope", decodeStart, nil)
		handlerStart := time.Now()
		value, err = b.applyBinaryRaftCommand(ctx, commandType, binaryBody)
		b.recordFSMStage(ctx, commandType, "handler", handlerStart, err)
		return value, err
	}
	var cmd raftCommand
	decodeErr := unmarshalJSON(data, &cmd)
	if decodeErr != nil {
		err = wrapBroker("raft_command_decode_failed", decodeErr, "decode raft command")
		b.recordFSMStage(ctx, commandType, "decode_envelope", decodeStart, err)
		return nil, err
	}
	commandType = cmd.Type
	b.recordFSMStage(ctx, commandType, "decode_envelope", decodeStart, nil)
	handler, ok := b.raftCommandHandlers().Get(cmd.Type)
	if !ok {
		err = brokerStoreError(store.CodeCodec, "unknown raft command %s", cmd.Type)
		b.recordFSMStage(ctx, commandType, "dispatch", time.Now(), err)
		return nil, err
	}
	handlerStart := time.Now()
	value, err = handler(ctx, b, cmd.Payload)
	b.recordFSMStage(ctx, commandType, "handler", handlerStart, err)
	return value, err
}

type raftCommandHandler func(context.Context, *Broker, jsonRawMessage) (any, error)

func setRaftHandler[R any, T any](
	handlers *collectionmapping.Map[string, raftCommandHandler],
	commandType string,
	apply func(context.Context, R) (T, error),
	label string,
) {
	handlers.Set(commandType, func(ctx context.Context, broker *Broker, payload jsonRawMessage) (any, error) {
		return decodeRaftCommand(ctx, broker, commandType, payload, apply, label)
	})
}

func (b *Broker) raftCommandHandlers() *collectionmapping.Map[string, raftCommandHandler] {
	handlers := collectionmapping.NewMap[string, raftCommandHandler]()
	b.registerTopicRaftHandlers(handlers)
	b.registerTransactionRaftHandlers(handlers)
	b.registerDirectRaftHandlers(handlers)
	b.registerGroupRaftHandlers(handlers)
	b.registerRetryDelayRaftHandlers(handlers)
	b.registerGovernanceRaftHandlers(handlers)
	return handlers
}

func (b *Broker) registerTopicRaftHandlers(handlers *collectionmapping.Map[string, raftCommandHandler]) {
	setRaftHandler(handlers, raftCommandCreateTopic, b.applyCreateTopic, "decode create topic command")
	setRaftHandler(handlers, raftCommandProduce, b.applyProduce, "decode produce command")
	setRaftHandler(handlers, raftCommandProduceBatch, b.applyProduceBatch, "decode produce batch command")
	setRaftHandler(handlers, raftCommandProduceBatches, b.applyProduceBatches, "decode produce batches command")
	setRaftHandler(handlers, raftCommandCommitOffset, b.applyCommitOffset, "decode commit offset command")
	setRaftHandler(handlers, raftCommandCommitOffsets, b.applyCommitOffsets, "decode commit offsets command")
}

func (b *Broker) registerTransactionRaftHandlers(handlers *collectionmapping.Map[string, raftCommandHandler]) {
	setRaftHandler(handlers, raftCommandTxBegin, b.applyTxBegin, "decode transaction begin command")
	setRaftHandler(handlers, raftCommandTxPublish, b.applyTxPublish, "decode transaction publish command")
	setRaftHandler(handlers, raftCommandTxPublishBatch, b.applyTxPublishBatch, "decode transaction publish batch command")
	setRaftHandler(handlers, raftCommandTxCommitOffset, b.applyTxCommitOffset, "decode transaction commit offset command")
	setRaftHandler(handlers, raftCommandTxCommit, b.applyTxCommit, "decode transaction commit command")
	setRaftHandler(handlers, raftCommandTxAbort, b.applyTxAbort, "decode transaction abort command")
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

func (b *Broker) registerGovernanceRaftHandlers(handlers *collectionmapping.Map[string, raftCommandHandler]) {
	setRaftHandler(handlers, raftCommandUpsertACLPolicy, b.applyUpsertACLPolicy, "decode upsert acl policy command")
	setRaftHandler(handlers, raftCommandDeleteACLPolicy, b.applyDeleteACLPolicy, "decode delete acl policy command")
}

func decodeRaftCommand[R any, T any](
	ctx context.Context,
	broker *Broker,
	commandType string,
	payload jsonRawMessage,
	apply func(context.Context, R) (T, error),
	label string,
) (any, error) {
	decodeStart := time.Now()
	var req R
	if err := unmarshalJSON(payload, &req); err != nil {
		wrapped := wrapBroker("raft_payload_decode_failed", err, "%s", label)
		broker.recordFSMStage(ctx, commandType, "decode_payload", decodeStart, wrapped)
		return nil, wrapped
	}
	broker.recordFSMStage(ctx, commandType, "decode_payload", decodeStart, nil)
	applyStart := time.Now()
	value, err := apply(ctx, req)
	broker.recordFSMStage(ctx, commandType, "apply", applyStart, err)
	return value, err
}

func (b *Broker) recordFSMStage(ctx context.Context, commandType, stage string, start time.Time, err error) {
	if b == nil || b.metrics == nil {
		return
	}
	b.metrics.RecordFSMStage(ctx, commandType, stage, time.Since(start), err)
}

func raftValueAs[T any](value any) (T, error) {
	var zero T
	typed, ok := value.(T)
	if ok {
		return typed, nil
	}
	marshaled, err := marshalJSON(value)
	if err != nil {
		return zero, wrapBroker("raft_result_encode_failed", err, "encode raft result")
	}
	if err := unmarshalJSON(marshaled, &typed); err != nil {
		return zero, wrapBroker("raft_result_decode_failed", err, "decode raft result")
	}
	return typed, nil
}
