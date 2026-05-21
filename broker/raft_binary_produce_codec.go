package broker

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	raftPartitioningDefault    uint8 = 0
	raftPartitioningExplicit   uint8 = 1
	raftPartitioningRoundRobin uint8 = 2
	raftPartitioningKeyHash    uint8 = 3
	raftPartitioningRoutingKey uint8 = 4
)

func encodeBinaryProduceBatchesCommand(req produceBatchesCommand) ([]byte, error) {
	writer := newRaftBinaryWriter()
	if err := writeBinaryProduceBatchRequests(writer, req.Requests); err != nil {
		return nil, err
	}
	return writer.bytes(), nil
}

func writeBinaryProduceBatchRequests(writer *raftBinaryWriter, requests []produceBatchCommand) error {
	count, err := checkedRaftUint32(len(requests), "produce batch requests")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, item := range requests {
		if err := writeBinaryProduceBatch(writer, item); err != nil {
			return err
		}
	}
	return nil
}

func writeBinaryProduceBatch(writer *raftBinaryWriter, item produceBatchCommand) error {
	if err := writer.writeString(item.Topic); err != nil {
		return err
	}
	writer.writeU8(encodeRaftPartitioning(item.Partitioning.Mode))
	writer.writeU32(item.Partitioning.Partition)
	writeBinaryProduceIdempotency(writer, item.Idempotency)
	recordCount, err := checkedRaftUint32(len(item.Records), "produce batch records")
	if err != nil {
		return err
	}
	writer.writeU32(recordCount)
	for _, record := range item.Records {
		if err := writeBinaryRecordAppend(writer, record); err != nil {
			return err
		}
	}
	return nil
}

func decodeBinaryProduceBatchesCommand(data []byte) (produceBatchesCommand, error) {
	reader := newRaftBinaryReader(data)
	requests, err := readBinaryProduceBatchRequests(reader)
	if err != nil {
		return produceBatchesCommand{}, err
	}
	if err := reader.ensureEOF(); err != nil {
		return produceBatchesCommand{}, err
	}
	return produceBatchesCommand{Requests: requests}, nil
}

func readBinaryProduceBatchRequests(reader *raftBinaryReader) ([]produceBatchCommand, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromRaftUint32(count)
	if err != nil {
		return nil, err
	}
	requests := collectionlist.NewListWithCapacity[produceBatchCommand](size)
	for range size {
		item, err := readBinaryProduceBatch(reader)
		if err != nil {
			return nil, err
		}
		requests.Add(item)
	}
	return requests.Values(), nil
}

func readBinaryProduceBatch(reader *raftBinaryReader) (produceBatchCommand, error) {
	topic, err := reader.readString()
	if err != nil {
		return produceBatchCommand{}, err
	}
	partitioning, err := readRaftPartitioning(reader)
	if err != nil {
		return produceBatchCommand{}, err
	}
	idempotency, err := readBinaryProduceIdempotency(reader)
	if err != nil {
		return produceBatchCommand{}, err
	}
	records, err := readBinaryRecordAppends(reader)
	if err != nil {
		return produceBatchCommand{}, err
	}
	return produceBatchCommand{
		Topic:        topic,
		Partitioning: partitioning,
		Records:      records,
		Idempotency:  idempotency,
	}, nil
}

func writeBinaryProduceIdempotency(writer *raftBinaryWriter, id *ProduceIdempotency) {
	if id == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeU64(id.ProducerID)
	writer.writeU64(id.ProducerEpoch)
	writer.writeU64(id.BaseSequence)
}

func readBinaryProduceIdempotency(reader *raftBinaryReader) (*ProduceIdempotency, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return nil, err
	}
	id := ProduceIdempotency{}
	if id.ProducerID, err = reader.readU64(); err != nil {
		return nil, err
	}
	if id.ProducerEpoch, err = reader.readU64(); err != nil {
		return nil, err
	}
	if id.BaseSequence, err = reader.readU64(); err != nil {
		return nil, err
	}
	return &id, nil
}

func writeBinaryRecordAppend(writer *raftBinaryWriter, record store.RecordAppend) error {
	writer.writeOptionalU64(record.TimestampMS)
	writer.writeOptionalU64(record.ExpiresAtMS)
	if err := writer.writeBytes(record.Key); err != nil {
		return err
	}
	if err := writeBinaryRecordHeaders(writer, record.Headers); err != nil {
		return err
	}
	writer.writeU16(record.Attributes)
	writeBinaryTransactionRecordMetadata(writer, record.Transaction)
	return writer.writeBytes(record.Payload)
}

func writeBinaryRecordHeaders(writer *raftBinaryWriter, headers []store.RecordHeader) error {
	count, err := checkedRaftUint16(len(headers), "record headers")
	if err != nil {
		return err
	}
	writer.writeU16(count)
	for _, header := range headers {
		if err := writer.writeString(header.Key); err != nil {
			return err
		}
		if err := writer.writeBytes(header.Value); err != nil {
			return err
		}
	}
	return nil
}

func readBinaryRecordAppends(reader *raftBinaryReader) ([]store.RecordAppend, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromRaftUint32(count)
	if err != nil {
		return nil, err
	}
	records := collectionlist.NewListWithCapacity[store.RecordAppend](size)
	for range size {
		record, err := readBinaryRecordAppend(reader)
		if err != nil {
			return nil, err
		}
		records.Add(record)
	}
	return records.Values(), nil
}

func readBinaryRecordAppend(reader *raftBinaryReader) (store.RecordAppend, error) {
	timestampMS, err := reader.readOptionalU64()
	if err != nil {
		return store.RecordAppend{}, err
	}
	expiresAtMS, err := reader.readOptionalU64()
	if err != nil {
		return store.RecordAppend{}, err
	}
	key, err := reader.readBytes()
	if err != nil {
		return store.RecordAppend{}, err
	}
	headers, err := readBinaryRecordHeaders(reader)
	if err != nil {
		return store.RecordAppend{}, err
	}
	attributes, err := reader.readU16()
	if err != nil {
		return store.RecordAppend{}, err
	}
	transaction, hasTransaction, err := readBinaryTransactionRecordMetadata(reader)
	if err != nil {
		return store.RecordAppend{}, err
	}
	payload, err := reader.readBytes()
	if err != nil {
		return store.RecordAppend{}, err
	}
	record := store.RecordAppend{
		TimestampMS: timestampMS,
		ExpiresAtMS: expiresAtMS,
		Key:         key,
		Headers:     headers,
		Attributes:  attributes,
		Payload:     payload,
	}
	if hasTransaction {
		record.Transaction = &transaction
	}
	return record, nil
}

func readBinaryRecordHeaders(reader *raftBinaryReader) ([]store.RecordHeader, error) {
	count, err := reader.readU16()
	if err != nil {
		return nil, err
	}
	headers := collectionlist.NewListWithCapacity[store.RecordHeader](int(count))
	for range count {
		key, err := reader.readString()
		if err != nil {
			return nil, err
		}
		value, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		headers.Add(store.RecordHeader{Key: key, Value: value})
	}
	return headers.Values(), nil
}

func encodeRaftPartitioning(mode string) uint8 {
	switch mode {
	case PartitionExplicit:
		return raftPartitioningExplicit
	case PartitionRoundRobin:
		return raftPartitioningRoundRobin
	case PartitionKeyHash:
		return raftPartitioningKeyHash
	case PartitionRoutingKeyHash:
		return raftPartitioningRoutingKey
	default:
		return raftPartitioningDefault
	}
}

func readRaftPartitioning(reader *raftBinaryReader) (PublishPartitioning, error) {
	mode, err := reader.readU8()
	if err != nil {
		return PublishPartitioning{}, err
	}
	partition, err := reader.readU32()
	if err != nil {
		return PublishPartitioning{}, err
	}
	switch mode {
	case raftPartitioningDefault:
		return PublishPartitioning{Partition: partition}, nil
	case raftPartitioningExplicit:
		return PublishPartitioning{Mode: PartitionExplicit, Partition: partition}, nil
	case raftPartitioningRoundRobin:
		return PublishPartitioning{Mode: PartitionRoundRobin, Partition: partition}, nil
	case raftPartitioningKeyHash:
		return PublishPartitioning{Mode: PartitionKeyHash, Partition: partition}, nil
	case raftPartitioningRoutingKey:
		return PublishPartitioning{Mode: PartitionRoutingKeyHash, Partition: partition}, nil
	default:
		return PublishPartitioning{}, brokerStoreError(store.CodeCodec, "unknown raft partitioning %d", mode)
	}
}
