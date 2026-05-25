package client

import (
	"hash/fnv"
	"sync/atomic"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
)

var nextProducerID atomic.Uint64

func ProducerPartition(partition uint32) ProducerOption {
	return func(producer *Producer) {
		producer.partition = clonePartition(&partition)
		producer.partitioning = protocol.ProducePartitioningExplicit
	}
}

func ProducerPartitioning(partitioning protocol.ProducePartitioning) ProducerOption {
	return func(producer *Producer) {
		if partitioning != "" {
			producer.partitioning = partitioning
		}
	}
}

func ProducerID(id uint64) ProducerOption {
	return func(producer *Producer) {
		if id != 0 {
			producer.producerID = id
		}
	}
}

func ProducerEpoch(epoch uint64) ProducerOption {
	return func(producer *Producer) {
		producer.epoch = epoch
	}
}

func ProducerDisableIdempotency() ProducerOption {
	return func(producer *Producer) {
		producer.idempotent = false
	}
}

func Key(key []byte) ProduceOption {
	return func(req *protocol.ProduceRequest) {
		req.Key = append([]byte(nil), key...)
	}
}

func RoutingKey(routingKey string) ProduceOption {
	return func(req *protocol.ProduceRequest) {
		req.RoutingKey = routingKey
		if req.Partitioning == protocol.ProducePartitioningRoundRobin {
			req.Partitioning = protocol.ProducePartitioningRoutingKeyHash
		}
	}
}

func Partition(partition uint32) ProduceOption {
	return func(req *protocol.ProduceRequest) {
		req.Partition = clonePartition(&partition)
		req.Partitioning = protocol.ProducePartitioningExplicit
	}
}

func Headers(headers ...protocol.MessageHeader) ProduceOption {
	return func(req *protocol.ProduceRequest) {
		req.Headers = cloneHeaders(headers)
	}
}

func Tombstone() ProduceOption {
	return func(req *protocol.ProduceRequest) {
		req.Tombstone = true
	}
}

func ExpiresAtMS(expiresAtMS uint64) ProduceOption {
	return func(req *protocol.ProduceRequest) {
		req.ExpiresAtMS = &expiresAtMS
	}
}

func ExpiresAfter(duration time.Duration) ProduceOption {
	return func(req *protocol.ProduceRequest) {
		if duration > 0 {
			expiresAt := unixMillis(time.Now().Add(duration))
			req.ExpiresAtMS = &expiresAt
		}
	}
}

func clonePartition(partition *uint32) *uint32 {
	if partition == nil {
		return nil
	}
	out := *partition
	return &out
}

func cloneHeaders(headers []protocol.MessageHeader) []protocol.MessageHeader {
	out := make([]protocol.MessageHeader, 0, len(headers))
	for _, header := range headers {
		out = append(out, protocol.MessageHeader{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	return out
}

func defaultProducerID() uint64 {
	next := nextProducerID.Add(1)
	hasher := fnv.New64a()
	if _, err := hasher.Write([]byte(time.Now().String())); err != nil {
		return next
	}
	if _, err := hasher.Write([]byte(defaultClientID())); err != nil {
		return next
	}
	id := hasher.Sum64()
	if id == 0 {
		return next
	}
	return id
}
