package ech0_test

import (
	"context"
	"errors"
	"testing"
	"time"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedDLQQueryAndReplay(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBrokerWithOptions(ctx, t, ech0.Options{
		DisableRetry: true,
		DisableDelay: true,
	})
	defer func() {
		requireNoError(t, b.Close(ctx))
	}()
	requireNoError(t, b.CreateTopic(ctx, "orders", ech0.Retry(ech0.RetryPolicy{
		MaxAttempts:    1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     time.Millisecond,
	})))
	sent, err := b.Publish(ctx, "orders", []byte("m1"), ech0.Key([]byte("k1")))
	requireNoError(t, err)
	requireNoError(t, b.Nack(ctx, "c1", sent, errors.New("worker failed")))
	time.Sleep(2 * time.Millisecond)
	moved, err := b.ProcessRetriesOnce(ctx)
	requireNoError(t, err)
	if moved != 1 {
		t.Fatalf("expected one retry record to move, got %d", moved)
	}
	query, err := b.QueryDLQ(ctx, "orders", ech0.DLQQuery{MaxRecords: 10, ErrorMessageContains: "failed"})
	requireNoError(t, err)
	if len(query.Records) != 1 || string(query.Records[0].Message.Payload) != "m1" {
		t.Fatalf("unexpected dlq query: %#v", query)
	}
	replayed, err := b.ReplayDLQ(ctx, "orders", query.Records[0].DLQPartition, query.Records[0].DLQOffset)
	requireNoError(t, err)
	if replayed.Message.Topic != "orders" || replayed.Message.Offset != 1 {
		t.Fatalf("unexpected replay result: %#v", replayed)
	}
}
