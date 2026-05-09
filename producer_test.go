package ech0_test

import (
	"context"
	"testing"
	"time"

	ech0 "github.com/DaiYuANg/ech0"
)

func TestEmbeddedProducerFlushesPartialBatchOnClose(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)
	requireNoError(t, b.CreateTopic(ctx, "orders", ech0.Partitions(2)))

	producer, err := b.NewProducer(ctx, "orders",
		ech0.ProducerBatchSize(4),
		ech0.ProducerLinger(time.Hour),
		ech0.ProducerInFlight(2),
	)
	requireNoError(t, err)

	futures := make([]*ech0.ProduceFuture, 0, 3)
	for _, payload := range [][]byte{[]byte("m1"), []byte("m2"), []byte("m3")} {
		future, sendErr := producer.Send(ctx, payload, ech0.Partition(1))
		requireNoError(t, sendErr)
		futures = append(futures, future)
	}
	requireNoError(t, producer.Close(ctx))

	for index, future := range futures {
		msg, awaitErr := future.Await(ctx)
		requireNoError(t, awaitErr)
		if msg.Partition != 1 || msg.Offset != uint64(index) {
			t.Fatalf("unexpected produced message: %#v", msg)
		}
	}
	fetched, err := b.Fetch(ctx, "c1", "orders", ech0.FetchPartition(1), ech0.FetchLimit(10))
	requireNoError(t, err)
	if len(fetched.Messages) != 3 {
		t.Fatalf("expected 3 messages, got %#v", fetched.Messages)
	}
}

func TestEmbeddedProducerFlushesFullBatchBeforeClose(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)
	requireNoError(t, b.CreateTopic(ctx, "orders"))

	producer, err := b.NewProducer(ctx, "orders",
		ech0.ProducerBatchSize(2),
		ech0.ProducerLinger(time.Hour),
	)
	requireNoError(t, err)
	defer func() {
		requireNoError(t, producer.Close(ctx))
	}()

	first, err := producer.Send(ctx, []byte("m1"))
	requireNoError(t, err)
	second, err := producer.Send(ctx, []byte("m2"))
	requireNoError(t, err)

	awaitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	firstMsg, err := first.Await(awaitCtx)
	requireNoError(t, err)
	secondMsg, err := second.Await(awaitCtx)
	requireNoError(t, err)
	if firstMsg.Offset != 0 || secondMsg.Offset != 1 {
		t.Fatalf("unexpected offsets: first=%#v second=%#v", firstMsg, secondMsg)
	}
}
