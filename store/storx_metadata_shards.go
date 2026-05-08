package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) SaveShardPlacement(placement ShardPlacement) error {
	if err := validateShardPlacement(placement); err != nil {
		return err
	}
	return wrapExternal(s.placements.Put(context.Background(), partitionKey(placement.TopicPartition()), cloneShardPlacement(placement)), "save shard placement")
}

func (s *StorxMetadataStore) LoadShardPlacement(topicPartition TopicPartition) (*ShardPlacement, error) {
	placement, ok, err := s.placements.Get(context.Background(), partitionKey(topicPartition))
	if err != nil {
		return nil, wrapExternal(err, "load shard placement")
	}
	if !ok {
		var absent *ShardPlacement
		return absent, nil
	}
	placement = cloneShardPlacement(placement)
	return &placement, nil
}

func (s *StorxMetadataStore) ListShardPlacements() ([]ShardPlacement, error) {
	out := collectionlist.NewList[ShardPlacement]()
	err := s.placements.Walk(context.Background(), func(entry bboltx.Entry[string, ShardPlacement]) error {
		out.Add(cloneShardPlacement(entry.Value))
		return nil
	})
	return sortShardPlacements(out.Values()), err
}
