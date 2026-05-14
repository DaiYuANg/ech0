package broker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arcgolabs/authx"
	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerTenantTopicsAreIsolated(t *testing.T) {
	b := newTestBroker(t)
	ctxA := tenantContext("tenant-a")
	ctxB := tenantContext("tenant-b")
	createTopic(ctxA, t, b, store.NewTopicConfig("orders"))
	createTopic(ctxB, t, b, store.NewTopicConfig("orders"))

	_, err := b.Publish(ctxA, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("a1"))
	requireNoError(t, err)
	_, err = b.Publish(ctxB, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("b1"))
	requireNoError(t, err)

	pollA, err := b.Fetch(ctxA, "c1", "orders", 0, nil, 10)
	requireNoError(t, err)
	if len(pollA.Records) != 1 || string(pollA.Records[0].Payload) != "a1" {
		t.Fatalf("unexpected tenant-a poll: %#v", pollA)
	}
	pollB, err := b.Fetch(ctxB, "c1", "orders", 0, nil, 10)
	requireNoError(t, err)
	if len(pollB.Records) != 1 || string(pollB.Records[0].Payload) != "b1" {
		t.Fatalf("unexpected tenant-b poll: %#v", pollB)
	}

	defaultTopics, err := b.ListTopicsFor(context.Background())
	requireNoError(t, err)
	if len(defaultTopics) != 0 {
		t.Fatalf("expected default tenant to see no tenant-scoped topics, got %#v", defaultTopics)
	}
}

func TestBrokerTenantDirectAndRequestReplyAreIsolated(t *testing.T) {
	b := newTestBroker(t)
	ctxA := tenantContext("tenant-a")
	ctxB := tenantContext("tenant-b")
	createTopic(ctxA, t, b, store.NewTopicConfig("svc.echo"))
	createTopic(ctxB, t, b, store.NewTopicConfig("svc.echo"))

	_, err := b.SendDirect(ctxA, "alice", "bob", nil, []byte("hello-a"))
	requireNoError(t, err)
	inboxB, err := b.FetchInboxFor(ctxB, "bob", 10)
	requireNoError(t, err)
	if len(inboxB.Records) != 0 {
		t.Fatalf("expected tenant-b inbox to be empty, got %#v", inboxB)
	}
	inboxA, err := b.FetchInboxFor(ctxA, "bob", 10)
	requireNoError(t, err)
	if len(inboxA.Records) != 1 || string(inboxA.Records[0].Message.Payload) != "hello-a" {
		t.Fatalf("unexpected tenant-a inbox: %#v", inboxA)
	}

	pending, err := b.StartRequest(ctxA, "svc.echo", []byte("ping"), broker.RequestOptions{
		InstanceID:   "a1",
		Timeout:      time.Second,
		PollInterval: time.Millisecond,
		Partitioning: broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0},
	})
	requireNoError(t, err)
	requestsB, err := b.FetchRequests(ctxB, "workers", "svc.echo", 0, nil, 1)
	requireNoError(t, err)
	if len(requestsB.Requests) != 0 {
		t.Fatalf("expected tenant-b request stream to be empty, got %#v", requestsB)
	}
	requestsA, err := b.FetchRequests(ctxA, "workers", "svc.echo", 0, nil, 1)
	requireNoError(t, err)
	if len(requestsA.Requests) != 1 {
		t.Fatalf("expected tenant-a request stream to have one request, got %#v", requestsA)
	}
	_, err = b.Reply(ctxA, requestsA.Requests[0], "b1", []byte("pong"))
	requireNoError(t, err)
	reply, err := b.AwaitReply(ctxA, pending)
	requireNoError(t, err)
	if string(reply.Payload) != "pong" {
		t.Fatalf("unexpected tenant-a reply: %#v", reply)
	}
}

func TestBrokerAuthxAuthorizerCanDenyProduce(t *testing.T) {
	authorizer := authx.AuthorizerFunc(func(_ context.Context, input authx.AuthorizationModel) (authx.Decision, error) {
		if input.Action == "produce" {
			return authx.Decision{Allowed: false, Reason: "produce_blocked", PolicyID: "test"}, nil
		}
		return authx.Decision{Allowed: true, PolicyID: "test"}, nil
	})
	b, err := broker.New(broker.DefaultConfig(), broker.WithACLAuthorizer(authorizer))
	requireNoError(t, err)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("m1"))
	if err == nil {
		t.Fatal("expected produce to be denied")
	}
}

func TestBrokerQuotaLimiterCanDenyProduce(t *testing.T) {
	b, err := broker.New(broker.DefaultConfig(), broker.WithQuotaLimiter(denyProduceQuota{}))
	requireNoError(t, err)
	ctx := context.Background()
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("m1"))
	if err == nil {
		t.Fatal("expected produce to be denied by quota")
	}
}

func tenantContext(tenant string) context.Context {
	ctx := broker.WithTenant(context.Background(), tenant)
	ctx = broker.WithNamespace(ctx, "default")
	return broker.WithPrincipal(ctx, "svc-"+tenant)
}

type denyProduceQuota struct{}

func (denyProduceQuota) CheckQuota(_ context.Context, req broker.QuotaRequest) error {
	if req.Action == broker.QuotaActionProduce {
		return errors.New("produce quota exceeded")
	}
	return nil
}
