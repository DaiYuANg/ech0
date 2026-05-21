package broker_test

import (
	"context"
	"errors"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

var errTestOutboxMark = errors.New("mark failed")

func TestDatabaseOutboxRowsPublishAndMarkDelivered(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	partition := uint32(0)
	marked := make([]string, 0, 1)

	result, err := b.ProcessDatabaseOutboxRows(ctx, broker.DatabaseOutboxConfig{
		Name:  "orders-outbox",
		Topic: "orders",
		Headers: []broker.WebhookSinkHeaderConfig{{
			Key:   "content-type",
			Value: "application/json",
		}},
	}, []broker.DatabaseOutboxRow{{
		ID:        "row-1",
		Partition: &partition,
		Payload:   []byte(`{"event":"created"}`),
	}}, func(_ context.Context, id string) error {
		marked = append(marked, id)
		return nil
	})
	requireNoError(t, err)
	if result.Published != 1 || result.Marked != 1 || len(marked) != 1 || marked[0] != "row-1" {
		t.Fatalf("unexpected outbox result=%#v marked=%#v", result, marked)
	}
	poll := fetchTopic(t, b, "c1", "orders", nil, 10)
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != `{"event":"created"}` {
		t.Fatalf("unexpected outbox poll: %#v", poll)
	}
	if headerValue(poll.Records[0].Headers, "content-type") != "application/json" {
		t.Fatalf("expected outbox static header, got %#v", poll.Records[0].Headers)
	}
}

func TestDatabaseOutboxMarkFailureStopsAfterPublish(t *testing.T) {
	ctx := context.Background()
	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	result, err := b.ProcessDatabaseOutboxRows(ctx, broker.DatabaseOutboxConfig{
		Name:  "orders-outbox",
		Topic: "orders",
	}, []broker.DatabaseOutboxRow{{
		ID:      "row-1",
		Payload: []byte("m1"),
	}}, func(context.Context, string) error {
		return errTestOutboxMark
	})
	if err == nil {
		t.Fatal("expected mark failure")
	}
	if result.Published != 1 || result.Marked != 0 {
		t.Fatalf("unexpected outbox result after mark failure: %#v", result)
	}
}
