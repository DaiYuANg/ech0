package broker

import (
	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

type shardPlacementStore interface {
	store.ShardPlacementStore
}

type brokerShardResolver struct {
	meta       metadataStore
	shardCount uint32
}

func newBrokerShardResolver(meta metadataStore, shardCount uint32) *brokerShardResolver {
	if shardCount == 0 {
		shardCount = 1
	}
	return &brokerShardResolver{meta: meta, shardCount: shardCount}
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
	if placements, ok := r.placementStore(); ok {
		placement, err := placements.LoadShardPlacement(tp)
		if err != nil {
			return store.ShardPlacement{}, wrapBrokerStore(err, "load shard placement")
		}
		if placement != nil {
			return *placement, nil
		}
	}
	return store.NewShardPlacement(tp.Topic, tp.Partition, r.shardForPartition(tp.Partition)), nil
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
			continue
		}
		if err := placements.SaveShardPlacement(placement); err != nil {
			return wrapBrokerStore(err, "save shard placement")
		}
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

func (b *Broker) ensureTopicShardPlacements(topic store.TopicConfig) error {
	if b == nil || b.shards == nil {
		return nil
	}
	return b.shards.EnsureTopic(topic)
}
