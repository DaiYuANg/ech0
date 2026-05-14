package ech0_test

import (
	"context"
	"testing"
)

func TestEmbeddedCommitOffsetStoresMetadata(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	publishEmbedded(ctx, t, b, []byte("m1"))
	fetched := fetchEmbedded(ctx, t, b)
	requireNoError(t, b.AckWithMetadata(ctx, "c1", fetched.Messages[0], "checkpoint=42"))

	state, err := b.CommittedOffset(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	if state == nil || state.NextOffset != 1 || state.Metadata != "checkpoint=42" || state.Topic != "orders" {
		t.Fatalf("unexpected committed offset state: %#v", state)
	}
}

func TestEmbeddedTransactionCommitOffsetStoresMetadata(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	publishEmbedded(ctx, t, b, []byte("m1"))
	fetched := fetchEmbedded(ctx, t, b)

	tx, err := b.BeginTransaction(ctx, "worker-1")
	requireNoError(t, err)
	requireNoError(t, tx.CommitOffsetWithMetadata(ctx, "c1", fetched.Messages[0], "tx-checkpoint=9"))
	requireNoError(t, tx.Commit(ctx))

	state, err := b.CommittedOffset(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	if state == nil || state.NextOffset != 1 || state.Metadata != "tx-checkpoint=9" {
		t.Fatalf("unexpected transaction offset state: %#v", state)
	}
}
