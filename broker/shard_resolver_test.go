package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerCreateTopicPersistsShardPlacements(t *testing.T) {
	meta := store.NewMemoryStore()
	logStore := store.NewMemoryStore()
	cfg := broker.DefaultConfig()
	cfg.Broker.DataShardCount = 3
	br, err := broker.NewWithStores(cfg, logStore, meta)
	requireNoError(t, err)

	topic := store.NewTopicConfig("orders")
	topic.Partitions = 5
	created, err := br.CreateTopic(context.Background(), topic)
	requireNoError(t, err)
	if created.Partitions != 5 {
		t.Fatalf("unexpected topic: %#v", created)
	}
	placements, err := meta.ListShardPlacements()
	requireNoError(t, err)
	if len(placements) != 5 {
		t.Fatalf("expected five shard placements, got %#v", placements)
	}
	for partition, wantShard := range []store.ShardID{0, 1, 2, 0, 1} {
		got, err := meta.LoadShardPlacement(store.NewTopicPartition("orders", uint32(partition)))
		requireNoError(t, err)
		if got == nil || got.ShardID != wantShard {
			t.Fatalf("unexpected placement for partition %d: %#v", partition, got)
		}
	}
}
