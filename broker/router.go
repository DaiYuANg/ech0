package broker

import (
	"hash/fnv"
	"sync"

	"github.com/DaiYuANg/ech0/store"
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
	cursors map[string]uint32
}

func newPartitionRouter() *partitionRouter {
	return &partitionRouter{cursors: make(map[string]uint32)}
}

func (r *partitionRouter) selectPartition(topic store.TopicConfig, partitioning PublishPartitioning, key []byte) (uint32, error) {
	if topic.Partitions == 0 {
		return 0, store.E(store.CodeInvalidArgument, "topic %s has zero partitions", topic.Name)
	}
	switch partitioning.Mode {
	case PartitionExplicit:
		if partitioning.Partition >= topic.Partitions {
			return 0, store.E(store.CodePartitionNotFound, "partition %s/%d not found", topic.Name, partitioning.Partition)
		}
		return partitioning.Partition, nil
	case PartitionKeyHash:
		if len(key) == 0 {
			return 0, store.E(store.CodeInvalidArgument, "key_hash partitioning requires a non-empty key")
		}
		hash := fnv.New64a()
		_, _ = hash.Write(key)
		return uint32(hash.Sum64() % uint64(topic.Partitions)), nil
	default:
		return r.nextRoundRobin(topic), nil
	}
}

func (r *partitionRouter) nextRoundRobin(topic store.TopicConfig) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	cursor := r.cursors[topic.Name]
	partition := cursor % topic.Partitions
	r.cursors[topic.Name] = (cursor + 1) % topic.Partitions
	return partition
}
