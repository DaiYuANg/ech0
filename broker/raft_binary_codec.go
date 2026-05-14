package broker

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/lyonbrown4d/ech0/store"
)

const (
	raftBinaryMagic uint32 = 0x45523031 // ER01

	raftBinaryCommandProduceBatches uint8 = 1
	raftBinaryCommandCommitOffsets  uint8 = 2
)

func encodeBinaryRaftCommand(commandType string, payload any) ([]byte, bool, error) {
	var commandID uint8
	var body []byte
	var err error
	switch commandType {
	case raftCommandProduceBatches:
		commandID = raftBinaryCommandProduceBatches
		req, castErr := raftBinaryPayloadAs[produceBatchesCommand](payload)
		if castErr != nil {
			return nil, true, castErr
		}
		body, err = encodeBinaryProduceBatchesCommand(req)
	case raftCommandCommitOffsets:
		commandID = raftBinaryCommandCommitOffsets
		req, castErr := raftBinaryPayloadAs[commitOffsetsCommand](payload)
		if castErr != nil {
			return nil, true, castErr
		}
		body, err = encodeBinaryCommitOffsetsCommand(req)
	default:
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	writer := newRaftBinaryWriter()
	writer.writeU32(raftBinaryMagic)
	writer.writeU8(commandID)
	writer.writeRaw(body)
	return writer.bytes(), true, nil
}

func decodeBinaryRaftEnvelope(data []byte) (string, []byte, bool, error) {
	if !isBinaryRaftCommand(data) {
		return "", nil, false, nil
	}
	reader := newRaftBinaryReader(data)
	if _, err := reader.readU32(); err != nil {
		return "", nil, true, err
	}
	commandID, err := reader.readU8()
	if err != nil {
		return "", nil, true, err
	}
	commandType, err := binaryRaftCommandType(commandID)
	if err != nil {
		return "", nil, true, err
	}
	return commandType, data[5:], true, nil
}

func isBinaryRaftCommand(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return binary.BigEndian.Uint32(data[:4]) == raftBinaryMagic
}

func (b *Broker) applyBinaryRaftCommand(ctx context.Context, commandType string, body []byte) (any, error) {
	decodeStart := time.Now()
	switch commandType {
	case raftCommandProduceBatches:
		req, err := decodeBinaryProduceBatchesCommand(body)
		if err != nil {
			wrapped := wrapBroker("raft_payload_decode_failed", err, "decode produce batches command")
			b.recordFSMStage(ctx, commandType, "decode_payload", decodeStart, wrapped)
			return nil, wrapped
		}
		b.recordFSMStage(ctx, commandType, "decode_payload", decodeStart, nil)
		return b.applyBinaryProduceBatches(ctx, req, commandType)
	case raftCommandCommitOffsets:
		req, err := decodeBinaryCommitOffsetsCommand(body)
		if err != nil {
			wrapped := wrapBroker("raft_payload_decode_failed", err, "decode commit offsets command")
			b.recordFSMStage(ctx, commandType, "decode_payload", decodeStart, wrapped)
			return nil, wrapped
		}
		b.recordFSMStage(ctx, commandType, "decode_payload", decodeStart, nil)
		return b.applyBinaryCommitOffsets(ctx, req, commandType)
	default:
		return nil, brokerStoreError(store.CodeCodec, "unknown binary raft command %s", commandType)
	}
}

func (b *Broker) applyBinaryProduceBatches(ctx context.Context, req produceBatchesCommand, commandType string) (any, error) {
	applyStart := time.Now()
	value, err := b.applyProduceBatches(ctx, req)
	b.recordFSMStage(ctx, commandType, "apply", applyStart, err)
	return value, err
}

func (b *Broker) applyBinaryCommitOffsets(ctx context.Context, req commitOffsetsCommand, commandType string) (any, error) {
	applyStart := time.Now()
	value, err := b.applyCommitOffsets(ctx, req)
	b.recordFSMStage(ctx, commandType, "apply", applyStart, err)
	return value, err
}

func binaryRaftCommandType(commandID uint8) (string, error) {
	switch commandID {
	case raftBinaryCommandProduceBatches:
		return raftCommandProduceBatches, nil
	case raftBinaryCommandCommitOffsets:
		return raftCommandCommitOffsets, nil
	default:
		return "", brokerStoreError(store.CodeCodec, "unknown binary raft command %d", commandID)
	}
}

func raftBinaryPayloadAs[T any](payload any) (T, error) {
	if typed, ok := payload.(T); ok {
		return typed, nil
	}
	if typed, ok := payload.(*T); ok && typed != nil {
		return *typed, nil
	}
	var zero T
	return zero, brokerStoreError(store.CodeCodec, "binary raft command payload type mismatch")
}
