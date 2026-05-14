package broker

import "github.com/lyonbrown4d/ech0/store"

func writeBinaryTransactionRecordMetadata(writer *raftBinaryWriter, metadata *store.TransactionRecordMetadata) {
	if metadata == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeU64(metadata.TxID)
	writer.writeU64(metadata.ProducerID)
	writer.writeU64(metadata.ProducerEpoch)
	writer.writeU64(metadata.Sequence)
	writer.writeU8(encodeRaftTransactionControl(metadata.ControlType))
}

func readBinaryTransactionRecordMetadata(reader *raftBinaryReader) (store.TransactionRecordMetadata, bool, error) {
	ok, err := reader.readBool()
	if err != nil {
		return store.TransactionRecordMetadata{}, false, err
	}
	if !ok {
		return store.TransactionRecordMetadata{}, false, nil
	}
	metadata := store.TransactionRecordMetadata{}
	if metadata.TxID, err = reader.readU64(); err != nil {
		return store.TransactionRecordMetadata{}, false, err
	}
	if metadata.ProducerID, err = reader.readU64(); err != nil {
		return store.TransactionRecordMetadata{}, false, err
	}
	if metadata.ProducerEpoch, err = reader.readU64(); err != nil {
		return store.TransactionRecordMetadata{}, false, err
	}
	if metadata.Sequence, err = reader.readU64(); err != nil {
		return store.TransactionRecordMetadata{}, false, err
	}
	if metadata.ControlType, err = readRaftTransactionControl(reader); err != nil {
		return store.TransactionRecordMetadata{}, false, err
	}
	return metadata, true, nil
}

func encodeRaftTransactionControl(controlType store.TransactionControlType) uint8 {
	switch controlType {
	case store.TransactionControlNone:
		return 0
	case store.TransactionControlCommit:
		return 1
	case store.TransactionControlAbort:
		return 2
	default:
		return 0
	}
}

func readRaftTransactionControl(reader *raftBinaryReader) (store.TransactionControlType, error) {
	value, err := reader.readU8()
	if err != nil {
		return "", err
	}
	switch value {
	case 0:
		return store.TransactionControlNone, nil
	case 1:
		return store.TransactionControlCommit, nil
	case 2:
		return store.TransactionControlAbort, nil
	default:
		return "", brokerStoreError(store.CodeCodec, "unknown raft transaction control type %d", value)
	}
}
