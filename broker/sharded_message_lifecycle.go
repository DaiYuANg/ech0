package broker

import (
	"errors"

	"github.com/lyonbrown4d/ech0/store"
)

func (r *shardedMessageRuntime) Close() error {
	if r == nil || r.shards == nil {
		return nil
	}
	var result error
	r.shards.Range(func(shardID store.ShardID, shard *messageShard) bool {
		if shard != nil {
			result = errors.Join(result, wrapBroker("message_shard_close_failed", shard.log.Close(), "close message shard %d", shardID))
		}
		return true
	})
	return result
}
