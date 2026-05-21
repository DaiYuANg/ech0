package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerReassignsEmptyPartitionPlacement(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Broker.DataShardCount = 2
	st := store.NewMemoryStore()
	b, err := broker.NewWithStores(cfg, st, st)
	requireNoError(t, err)

	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)

	requireNoError(t, b.ReassignPartition(ctx, "orders", 1, 0))
	placement, err := st.LoadShardPlacement(store.NewTopicPartition("orders", 1))
	requireNoError(t, err)
	if placement == nil || placement.ShardID != 0 {
		t.Fatalf("unexpected placement: %#v", placement)
	}
}

func TestBrokerRejectsLivePartitionReassignment(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Broker.DataShardCount = 2
	st := store.NewMemoryStore()
	b, err := broker.NewWithStores(cfg, st, st)
	requireNoError(t, err)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	if err := b.ReassignPartition(ctx, "orders", 0, 1); err == nil {
		t.Fatal("expected live partition reassignment to be rejected")
	}
}
