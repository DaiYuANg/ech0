package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerRoutingKeyHashPersistsAndSelectsPartition(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.TopicConfig{Name: "orders", Partitions: 4})

	partitioning := broker.PublishPartitioning{Mode: broker.PartitionRoutingKeyHash, RoutingKey: "customer.eu"}
	first, err := b.Publish(ctx, "orders", partitioning, nil, false, []byte("m1"))
	requireNoError(t, err)
	second, err := b.Publish(ctx, "orders", partitioning, nil, false, []byte("m2"))
	requireNoError(t, err)
	if first.Partition != second.Partition {
		t.Fatalf("routing key should select stable partition, got %d and %d", first.Partition, second.Partition)
	}

	poll, err := b.Fetch(ctx, "c1", "orders", first.Partition, nil, 10)
	requireNoError(t, err)
	if len(poll.Records) != 2 || headerValue(poll.Records[0].Headers, broker.RoutingKeyHeader) != "customer.eu" {
		t.Fatalf("unexpected routing-key poll result: %#v", poll)
	}
	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionRoutingKeyHash}, nil, false, []byte("bad"))
	if err == nil {
		t.Fatal("expected empty routing key to fail")
	}
}

func TestBrokerFanoutPublishesToEveryPartition(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.TopicConfig{Name: "events", Partitions: 3})

	record := store.NewRecordAppend([]byte("broadcast"))
	result, err := b.PublishFanoutRecord(ctx, "events", record)
	requireNoError(t, err)
	if len(result.Records) != 3 {
		t.Fatalf("expected fanout result for three partitions, got %#v", result)
	}
	for partition := range uint32(3) {
		poll, err := b.Fetch(ctx, "c1", "events", partition, nil, 10)
		requireNoError(t, err)
		if len(poll.Records) != 1 || string(poll.Records[0].Payload) != "broadcast" {
			t.Fatalf("unexpected fanout records on partition %d: %#v", partition, poll)
		}
	}
}
