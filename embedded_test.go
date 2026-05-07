package ech0

import (
	"context"
	"testing"
)

func TestEmbeddedBrokerMinimalFlow(t *testing.T) {
	ctx := context.Background()
	b, err := Open(ctx, Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := b.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	if err := b.CreateTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	produced, err := b.Publish(ctx, "orders", []byte("m1"))
	if err != nil {
		t.Fatal(err)
	}
	if produced.Topic != "orders" || produced.NextOffset != produced.Offset+1 {
		t.Fatalf("unexpected produced message: %#v", produced)
	}
	fetched, err := b.Fetch(ctx, "c1", "orders", FetchLimit(10))
	if err != nil {
		t.Fatal(err)
	}
	if len(fetched.Messages) != 1 || string(fetched.Messages[0].Payload) != "m1" {
		t.Fatalf("unexpected fetched messages: %#v", fetched)
	}
	if err := b.Ack(ctx, "c1", fetched.Messages[0]); err != nil {
		t.Fatal(err)
	}
	fetched, err = b.Fetch(ctx, "c1", "orders", FetchLimit(10))
	if err != nil {
		t.Fatal(err)
	}
	if len(fetched.Messages) != 0 {
		t.Fatalf("expected no messages after ack, got %#v", fetched)
	}
}
