package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
)

func TestTCPFetchSubjectPattern(t *testing.T) {
	b := newTestBroker(t)
	ctx := context.Background()
	server := broker.NewTCPServer(broker.DefaultConfig(), b, nil, nil)
	createTopic(ctx, t, b, store.NewTopicConfig("orders.created"))
	createTopic(ctx, t, b, store.NewTopicConfig("orders.canceled"))
	createTopic(ctx, t, b, store.NewTopicConfig("payments.created"))
	_, err := b.Publish(ctx, "orders.created", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("created"))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "orders.canceled", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("canceled"))
	requireNoError(t, err)
	_, err = b.Publish(ctx, "payments.created", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("payment"))
	requireNoError(t, err)

	topics := handleProtocolFrame[protocol.ListTopicsResponse](ctx, t, server, protocol.CmdListTopicsRequest, protocol.CmdListTopicsResponse, protocol.ListTopicsRequest{
		Pattern: "orders.*",
	})
	if len(topics.Topics) != 2 {
		t.Fatalf("expected two topics, got %#v", topics)
	}

	fetched := handleProtocolFrame[protocol.FetchSubjectPatternResponse](ctx, t, server, protocol.CmdFetchSubjectPatternRequest, protocol.CmdFetchSubjectPatternResponse, protocol.FetchSubjectPatternRequest{
		Consumer:   "worker",
		Pattern:    "orders.*",
		MaxRecords: 10,
	})
	if len(fetched.Items) != 2 {
		t.Fatalf("expected two subject fetch items, got %#v", fetched)
	}
	payloads := map[string]string{}
	for index := range fetched.Items {
		item := &fetched.Items[index]
		if len(item.Records) != 1 {
			t.Fatalf("unexpected subject fetch item: %#v", item)
		}
		payloads[item.Topic] = string(item.Records[0].Payload)
	}
	if payloads["orders.created"] != "created" || payloads["orders.canceled"] != "canceled" {
		t.Fatalf("unexpected subject payloads: %#v", payloads)
	}
}
