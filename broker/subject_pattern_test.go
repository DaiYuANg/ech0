package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestSubjectPatternMatchesNATSStyleWildcards(t *testing.T) {
	cases := []struct {
		pattern string
		subject string
		want    bool
	}{
		{pattern: "orders.*", subject: "orders.created", want: true},
		{pattern: "orders.*", subject: "orders.eu.created", want: false},
		{pattern: "orders.>", subject: "orders.eu.created", want: true},
		{pattern: "orders.>", subject: "orders", want: false},
		{pattern: ">", subject: "orders.created", want: true},
		{pattern: "payments.*", subject: "orders.created", want: false},
	}
	for _, tc := range cases {
		got, err := broker.SubjectPatternMatches(tc.pattern, tc.subject)
		requireNoError(t, err)
		if got != tc.want {
			t.Fatalf("SubjectPatternMatches(%q, %q) = %t, want %t", tc.pattern, tc.subject, got, tc.want)
		}
	}
}

func TestBrokerFetchSubjectPattern(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders.created"))
	createTopic(ctx, t, b, store.NewTopicConfig("orders.canceled"))
	createTopic(ctx, t, b, store.NewTopicConfig("payments.created"))
	_, err := b.Publish(ctx, "orders.created", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("created"))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "orders.canceled", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("canceled"))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "payments.created", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("payment"))
	requireNoError(t, err)

	result, err := b.FetchSubjectPatternWithIsolation(ctx, "worker", "orders.*", 10, broker.FetchIsolationReadUncommitted)
	requireNoError(t, err)
	if len(result.Items) != 2 {
		t.Fatalf("expected two matching subject partitions, got %#v", result)
	}
	payloads := map[string]string{}
	for index := range result.Items {
		item := &result.Items[index]
		if len(item.Poll.Records) != 1 {
			t.Fatalf("unexpected item: %#v", item)
		}
		payloads[item.Subject] = string(item.Poll.Records[0].Payload)
	}
	if payloads["orders.created"] != "created" || payloads["orders.canceled"] != "canceled" {
		t.Fatalf("unexpected wildcard payloads: %#v", payloads)
	}
}
