package ech0_test

import (
	"context"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedPauseResumeConsumer(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders"))
	publishEmbedded(ctx, t, b, []byte("m1"))

	paused, err := b.PauseConsumer(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	if !paused.Paused || paused.Topic != "orders" {
		t.Fatalf("unexpected pause result: %#v", paused)
	}
	empty, err := b.Fetch(ctx, "c1", "orders", ech0.FetchLimit(10))
	requireNoError(t, err)
	if len(empty.Messages) != 0 || empty.NextOffset != 0 {
		t.Fatalf("expected paused fetch to stay empty, got %#v", empty)
	}

	_, err = b.ResumeConsumer(ctx, "c1", "orders", 0)
	requireNoError(t, err)
	fetched := fetchEmbedded(ctx, t, b)
	if len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected fetched messages after resume: %#v", fetched)
	}
}
