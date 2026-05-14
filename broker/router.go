package broker

import (
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/cespare/xxhash/v2"
	"github.com/lyonbrown4d/ech0/store"
)

type PublishPartitioning struct {
	Mode      string
	Partition uint32
}

const (
	PartitionExplicit   = "explicit"
	PartitionRoundRobin = "round_robin"
	PartitionKeyHash    = "key_hash"
)

type partitionRouter struct {
	mu      sync.Mutex
	cursors *collectionmapping.Map[string, uint32]
}

func newPartitionRouter() *partitionRouter {
	return &partitionRouter{cursors: collectionmapping.NewMap[string, uint32]()}
}

func (r *partitionRouter) selectPartition(topic store.TopicConfig, partitioning PublishPartitioning, key []byte) (uint32, error) {
	if topic.Partitions == 0 {
		return 0, brokerStoreError(store.CodeInvalidArgument, "topic %s has zero partitions", topic.Name)
	}
	switch partitioning.Mode {
	case PartitionExplicit:
		if partitioning.Partition >= topic.Partitions {
			return 0, brokerStoreError(store.CodePartitionNotFound, "partition %s/%d not found", topic.Name, partitioning.Partition)
		}
		return partitioning.Partition, nil
	case PartitionKeyHash:
		if len(key) == 0 {
			return 0, brokerStoreError(store.CodeInvalidArgument, "key_hash partitioning requires a non-empty key")
		}
		partition := xxhash.Sum64(key) % uint64(topic.Partitions)
		return safeUint64ToUint32(partition), nil
	default:
		return r.nextRoundRobin(topic), nil
	}
}

func safeUint64ToUint32(value uint64) uint32 {
	const maxUint32 = ^uint32(0)
	if value > uint64(maxUint32) {
		return maxUint32
	}
	return uint32(value)
}

func (r *partitionRouter) nextRoundRobin(topic store.TopicConfig) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	cursor := r.cursors.GetOrDefault(topic.Name, 0)
	partition := cursor % topic.Partitions
	r.cursors.Set(topic.Name, (cursor+1)%topic.Partitions)
	return partition
}
