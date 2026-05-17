package broker_test

import (
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerTenantDefaultsApplyToTopicPolicies(t *testing.T) {
	retentionMS := uint64(10_000)
	delayEnabled := true
	cfg := broker.DefaultConfig()
	cfg.Governance.TenantDefaults = []broker.TenantDefaultsConfig{{
		Tenant:            "tenant-a",
		Namespace:         "default",
		RetentionMaxBytes: 4096,
		RetentionMS:       &retentionMS,
		RetryPolicy:       store.TopicRetryPolicy{MaxAttempts: 3, BackoffInitialMS: 7, BackoffMaxMS: 70, BackoffJitterFactor: 0.3},
		DeadLetterTopic:   "tenant-dlq",
		DelayEnabled:      &delayEnabled,
	}}
	b, err := broker.New(cfg)
	requireNoError(t, err)

	created, err := b.CreateTopic(tenantContext("tenant-a"), store.NewTopicConfig("orders"))
	requireNoError(t, err)
	requireTenantTopicDefaults(t, created, retentionMS)
	requireExplicitTenantTopicPolicyWins(t, b)
}

func requireTenantTopicDefaults(t *testing.T, topic store.TopicConfig, retentionMS uint64) {
	t.Helper()
	if topic.RetentionMaxBytes != 4096 {
		t.Fatalf("unexpected retention bytes: %#v", topic)
	}
	if topic.RetentionMS == nil || *topic.RetentionMS != retentionMS {
		t.Fatalf("unexpected retention ms: %#v", topic)
	}
	if topic.RetryPolicy != (store.TopicRetryPolicy{MaxAttempts: 3, BackoffInitialMS: 7, BackoffMaxMS: 70, BackoffJitterFactor: 0.3}) {
		t.Fatalf("unexpected retry defaults: %#v", topic.RetryPolicy)
	}
	if topic.DeadLetterTopic == nil || *topic.DeadLetterTopic != "tenant-dlq" {
		t.Fatalf("unexpected dlq default: %#v", topic)
	}
	if !topic.DelayEnabled {
		t.Fatalf("expected delay default to be enabled: %#v", topic)
	}
}

func requireExplicitTenantTopicPolicyWins(t *testing.T, b *broker.Broker) {
	t.Helper()
	explicit := store.NewTopicConfig("explicit")
	explicit.RetentionMaxBytes = 8192
	created, err := b.CreateTopic(tenantContext("tenant-a"), explicit)
	requireNoError(t, err)
	if created.RetentionMaxBytes != 8192 {
		t.Fatalf("expected explicit retention bytes to win, got %d", created.RetentionMaxBytes)
	}
}

func TestBrokerConfigQuotaEnforcesTenantStorageLimit(t *testing.T) {
	record := store.NewRecordAppend([]byte("abc"))
	cfg := broker.DefaultConfig()
	cfg.Governance.Quota.MaxStorageBytes = store.RecordAppendStorageBytes(record)
	b, err := broker.New(cfg)
	requireNoError(t, err)
	ctx := tenantContext("tenant-a")
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))

	_, err = b.PublishRecord(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, record)
	requireNoError(t, err)
	_, err = b.PublishRecord(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, record)
	if err == nil {
		t.Fatal("expected storage quota to be exceeded")
	}
}

func TestBrokerQuotaSummaryReportsTenantUsage(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Governance.Quota.MaxTopics = 3
	cfg.Governance.Quota.MaxStorageBytes = 1024
	b, err := broker.New(cfg)
	requireNoError(t, err)
	ctx := tenantContext("tenant-a")
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	_, err = b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, nil, false, []byte("m1"))
	requireNoError(t, err)

	summary, err := b.QuotaSummaryFor(ctx)
	requireNoError(t, err)
	requireTenantQuotaUsage(t, summary)
}

func requireTenantQuotaUsage(t *testing.T, summary broker.QuotaSummary) {
	t.Helper()
	if summary.Identity.Tenant != "tenant-a" {
		t.Fatalf("unexpected quota identity: %#v", summary.Identity)
	}
	if summary.CurrentTopics != 1 || summary.CurrentPartitions != 1 || summary.CurrentStorageBytes == 0 {
		t.Fatalf("unexpected quota usage: %#v", summary)
	}
	if summary.Limits.MaxTopics != 3 || summary.Limits.MaxStorageBytes != 1024 {
		t.Fatalf("unexpected quota limits: %#v", summary.Limits)
	}
}
