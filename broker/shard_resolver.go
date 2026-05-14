package broker

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
)

type shardPlacementStore interface {
	store.ShardPlacementStore
}

type brokerShardResolver struct {
	meta       metadataStore
	shardCount uint32
	cache      *collectionmapping.ShardedConcurrentMap[string, store.ShardPlacement]
}

func newBrokerShardResolver(meta metadataStore, shardCount uint32) *brokerShardResolver {
	if shardCount == 0 {
		shardCount = 1
	}
	return &brokerShardResolver{
		meta:       meta,
		shardCount: shardCount,
		cache:      collectionmapping.NewShardedConcurrentMap[string, store.ShardPlacement](32, collectionmapping.HashString),
	}
}

func (r *brokerShardResolver) PlanTopic(topic store.TopicConfig) []store.ShardPlacement {
	if topic.Partitions == 0 {
		topic.Partitions = 1
	}
	placements := collectionlist.NewListWithCapacity[store.ShardPlacement](int(topic.Partitions))
	for partition := range topic.Partitions {
		placements.Add(store.NewShardPlacement(topic.Name, partition, r.shardForPartition(partition)))
	}
	return placements.Values()
}

func (r *brokerShardResolver) Resolve(tp store.TopicPartition) (store.ShardPlacement, error) {
	if r.cache != nil {
		if cached, ok := r.cache.Get(topicPartitionCacheKey(tp)); ok {
			return cached, nil
		}
	}
	if placements, ok := r.placementStore(); ok {
		placement, err := placements.LoadShardPlacement(tp)
		if err != nil {
			return store.ShardPlacement{}, wrapBrokerStore(err, "load shard placement")
		}
		if placement != nil {
			r.cacheShardPlacement(*placement)
			return *placement, nil
		}
	}
	placement := store.NewShardPlacement(tp.Topic, tp.Partition, r.shardForPartition(tp.Partition))
	r.cacheShardPlacement(placement)
	return placement, nil
}

func (r *brokerShardResolver) EnsureTopic(topic store.TopicConfig) error {
	placements, ok := r.placementStore()
	if !ok {
		return nil
	}
	for _, placement := range r.PlanTopic(topic) {
		existing, err := placements.LoadShardPlacement(placement.TopicPartition())
		if err != nil {
			return wrapBrokerStore(err, "load shard placement")
		}
		if existing != nil {
			r.cacheShardPlacement(*existing)
			continue
		}
		if err := placements.SaveShardPlacement(placement); err != nil {
			return wrapBrokerStore(err, "save shard placement")
		}
		r.cacheShardPlacement(placement)
	}
	return nil
}

func (r *brokerShardResolver) shardForPartition(partition uint32) store.ShardID {
	return store.ShardID(partition % r.shardCount)
}

func (r *brokerShardResolver) placementStore() (shardPlacementStore, bool) {
	if r == nil || r.meta == nil {
		return nil, false
	}
	placements, ok := r.meta.(shardPlacementStore)
	return placements, ok
}

func (r *brokerShardResolver) cacheShardPlacement(placement store.ShardPlacement) {
	if r == nil || r.cache == nil {
		return
	}
	r.cache.Set(topicPartitionCacheKey(placement.TopicPartition()), placement)
}

func topicPartitionCacheKey(tp store.TopicPartition) string {
	return tp.Topic + "\x00" + strconvUint32(tp.Partition)
}

func (b *Broker) ensureTopicShardPlacements(topic store.TopicConfig) error {
	if b == nil || b.shards == nil {
		return nil
	}
	return b.shards.EnsureTopic(topic)
}
