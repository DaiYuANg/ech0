package ech0_test

import (
	"context"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedFanoutPublishesToEveryPartition(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "events", ech0.Partitions(2)))
	messages, err := b.PublishFanout(ctx, "events", []byte("broadcast"), ech0.RoutingKey("tenant.updated"))
	requireNoError(t, err)
	if len(messages) != 2 {
		t.Fatalf("expected two fanout messages, got %#v", messages)
	}
	for partition := range uint32(2) {
		fetched, err := b.Fetch(ctx, "c1", "events", ech0.FetchPartition(partition), ech0.FetchLimit(10))
		requireNoError(t, err)
		if len(fetched.Messages) != 1 || fetched.Messages[0].RoutingKey != "tenant.updated" {
			t.Fatalf("unexpected embedded fanout partition %d: %#v", partition, fetched)
		}
	}
}
