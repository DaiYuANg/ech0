package store

import (
	"cmp"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

func validateShardPlacement(placement ShardPlacement) error {
	if placement.Topic == "" {
		return E(CodeInvalidArgument, "shard placement topic is required")
	}
	return nil
}

func cloneShardPlacement(placement ShardPlacement) ShardPlacement {
	return placement
}

func sortShardPlacements(placements []ShardPlacement) []ShardPlacement {
	return collectionlist.NewList(placements...).
		Sort(func(left, right ShardPlacement) int {
			if left.Topic == right.Topic {
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).
		Values()
}
