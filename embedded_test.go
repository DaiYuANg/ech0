//nolint:testpackage // Same-package tests validate the root package API without import cycles.
package ech0

import (
	"context"
	"testing"
)

//nolint:cyclop,gocyclo,gocognit // This test keeps the embedded broker happy path linear.
func TestEmbeddedBrokerMinimalFlow(t *testing.T) {
	ctx := context.Background()
	b, err := Open(ctx, Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := b.Close(ctx); closeErr != nil {
			t.Fatal(closeErr)
		}
	}()

	if createErr := b.CreateTopic(ctx, "orders"); createErr != nil {
		t.Fatal(createErr)
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
	if ackErr := b.Ack(ctx, "c1", fetched.Messages[0]); ackErr != nil {
		t.Fatal(ackErr)
	}
	fetched, err = b.Fetch(ctx, "c1", "orders", FetchLimit(10))
	if err != nil {
		t.Fatal(err)
	}
	if len(fetched.Messages) != 0 {
		t.Fatalf("expected no messages after ack, got %#v", fetched)
	}
}
