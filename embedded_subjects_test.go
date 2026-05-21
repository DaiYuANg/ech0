package ech0_test

import (
	"context"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
)

func TestEmbeddedFetchSubjectsUsesWildcardPattern(t *testing.T) {
	ctx := context.Background()
	b := openEmbeddedBroker(ctx, t)
	defer closeEmbeddedBroker(ctx, t, b)

	requireNoError(t, b.CreateTopic(ctx, "orders.created"))
	requireNoError(t, b.CreateTopic(ctx, "orders.canceled"))
	requireNoError(t, b.CreateTopic(ctx, "payments.created"))
	_, err := b.Publish(ctx, "orders.created", []byte("created"))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "orders.canceled", []byte("canceled"))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "payments.created", []byte("payment"))
	requireNoError(t, err)

	subjects, err := b.ListSubjects(ctx, "orders.*")
	requireNoError(t, err)
	if len(subjects) != 2 {
		t.Fatalf("expected two subjects, got %#v", subjects)
	}

	result, err := b.FetchSubjects(ctx, "worker", "orders.*", ech0.FetchLimit(10))
	requireNoError(t, err)
	if len(result.Items) != 2 {
		t.Fatalf("expected two subject fetch items, got %#v", result)
	}
	payloads := map[string]string{}
	for index := range result.Items {
		item := &result.Items[index]
		if len(item.Messages) != 1 {
			t.Fatalf("unexpected subject item: %#v", item)
		}
		payloads[item.Subject] = string(item.Messages[0].Payload)
	}
	if payloads["orders.created"] != "created" || payloads["orders.canceled"] != "canceled" {
		t.Fatalf("unexpected subject payloads: %#v", payloads)
	}
}
