package broker

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

type dataShardSpec struct {
	ShardID        store.ShardID
	Dir            string
	SegmentLogPath string
}

func buildDataShardSpecs(cfg Config) []dataShardSpec {
	shardCount := cfg.Broker.DataShardCount
	if shardCount == 0 {
		shardCount = 1
	}
	specs := collectionlist.NewListWithCapacity[dataShardSpec](int(shardCount))
	for shardIndex := range shardCount {
		shardID := store.ShardID(shardIndex)
		specs.Add(dataShardSpec{
			ShardID:        shardID,
			Dir:            cfg.ShardDir(shardID),
			SegmentLogPath: cfg.ShardSegmentLogPath(shardID),
		})
	}
	return specs.Values()
}
