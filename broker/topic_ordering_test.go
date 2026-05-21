package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestTopicOrderingPolicyRoutesKeyToStablePartition(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 8
	topic.OrderingPolicy = store.TopicOrderingKey
	createTopic(ctx, t, b, topic)

	first, err := b.Publish(ctx, "orders", broker.PublishPartitioning{}, []byte("customer-1"), false, []byte("a"))
	requireNoError(t, err)
	second, err := b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, []byte("customer-1"), false, []byte("b"))
	requireNoError(t, err)
	if first.Partition != second.Partition {
		t.Fatalf("expected same key to keep partition, got %d then %d", first.Partition, second.Partition)
	}

	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{}, nil, false, []byte("missing-key"))
	if err == nil {
		t.Fatal("expected key-ordered topic to reject missing keys")
	}
}

func TestTopicOrderingPolicyRejectsMixedKeyBatch(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 8
	topic.OrderingPolicy = store.TopicOrderingKey
	createTopic(ctx, t, b, topic)

	_, err := b.PublishBatch(ctx, "orders", broker.PublishPartitioning{}, []store.RecordAppend{
		{Key: []byte("customer-1"), Payload: []byte("a")},
		{Key: []byte("customer-2"), Payload: []byte("b")},
	})
	if err == nil {
		t.Fatal("expected key-ordered topic to reject mixed-key batches")
	}
}

func TestTopicOrderingPolicyRoutesRoutingKeyToStablePartition(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 8
	topic.OrderingPolicy = store.TopicOrderingRoutingKey
	createTopic(ctx, t, b, topic)

	partitioning := broker.PublishPartitioning{RoutingKey: "customer.eu"}
	first, err := b.Publish(ctx, "orders", partitioning, nil, false, []byte("a"))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{}, nil, false, []byte("b"))
	if err == nil {
		t.Fatal("expected routing-key ordered topic to reject missing routing key")
	}

	second, err := b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0, RoutingKey: "customer.eu"}, nil, false, []byte("b"))
	requireNoError(t, err)
	if first.Partition != second.Partition {
		t.Fatalf("expected same routing key to keep partition, got %d then %d", first.Partition, second.Partition)
	}
}
