package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerACLPolicyCanDenyProduce(t *testing.T) {
	b := newTestBroker(t)
	ctxA := tenantContext("tenant-a")
	ctxB := tenantContext("tenant-b")
	createTopic(ctxA, t, b, store.NewTopicConfig("orders"))
	createTopic(ctxB, t, b, store.NewTopicConfig("orders"))

	policy, err := b.UpsertACLPolicy(context.Background(), broker.ACLPolicy{
		Tenant:       "tenant-a",
		Namespace:    "default",
		Principal:    "svc-tenant-a",
		ResourceType: broker.ACLResourceTopic,
		ResourceName: "orders",
		Actions:      []broker.ACLAction{broker.ACLActionProduce},
		Effect:       broker.ACLPolicyEffectDeny,
		Priority:     100,
	})
	requireNoError(t, err)

	_, err = b.Publish(ctxA, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("blocked"))
	if err == nil {
		t.Fatal("expected tenant-a produce to be denied by acl policy")
	}
	_, err = b.Publish(ctxB, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("allowed"))
	requireNoError(t, err)

	policies, err := b.ListACLPolicies(context.Background(), broker.ACLPolicyFilter{Tenant: "tenant-a"})
	requireNoError(t, err)
	if len(policies) != 1 || policies[0].PolicyID != policy.PolicyID {
		t.Fatalf("unexpected acl policies: %#v", policies)
	}
	requireNoError(t, b.DeleteACLPolicy(context.Background(), policy.PolicyID))
	_, err = b.Publish(ctxA, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("allowed-after-delete"))
	requireNoError(t, err)
}

func TestBrokerACLPolicyRequiresMatchWhenTenantHasPolicies(t *testing.T) {
	b := newTestBroker(t)
	ctx := tenantContext("tenant-a")
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	_, err := b.UpsertACLPolicy(context.Background(), broker.ACLPolicy{
		Tenant:       "tenant-a",
		Namespace:    "default",
		Principal:    "svc-tenant-a",
		ResourceType: broker.ACLResourceTopic,
		ResourceName: "orders",
		Actions:      []broker.ACLAction{broker.ACLActionProduce},
		Effect:       broker.ACLPolicyEffectAllow,
	})
	requireNoError(t, err)

	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("allowed"))
	requireNoError(t, err)
	other := broker.WithPrincipal(ctx, "svc-other")
	_, err = b.Publish(other, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("blocked"))
	if err == nil {
		t.Fatal("expected unmatched principal to be denied once tenant has acl policies")
	}
}

func TestBrokerACLPolicyDenyWinsAtSamePriority(t *testing.T) {
	b := newTestBroker(t)
	ctx := tenantContext("tenant-a")
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	base := broker.ACLPolicy{
		Tenant:       "tenant-a",
		Namespace:    "default",
		Principal:    "svc-tenant-a",
		ResourceType: broker.ACLResourceTopic,
		ResourceName: "orders",
		Actions:      []broker.ACLAction{broker.ACLActionProduce},
		Priority:     10,
	}
	allow := base
	allow.PolicyID = "allow-produce"
	allow.Effect = broker.ACLPolicyEffectAllow
	_, err := b.UpsertACLPolicy(context.Background(), allow)
	requireNoError(t, err)
	deny := base
	deny.PolicyID = "deny-produce"
	deny.Effect = broker.ACLPolicyEffectDeny
	_, err = b.UpsertACLPolicy(context.Background(), deny)
	requireNoError(t, err)

	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("blocked"))
	if err == nil {
		t.Fatal("expected deny acl policy to win at the same priority")
	}
}
