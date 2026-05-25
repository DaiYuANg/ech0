package broker_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestAdminGovernanceUIRendersTenantControls(t *testing.T) {
	ctx := context.Background()
	retentionMS := uint64(60000)
	messageTTLMS := uint64(120000)
	delayEnabled := true
	cfg := broker.DefaultConfig()
	cfg.Admin.Enabled = true
	cfg.Admin.BindAddr = freeTCPAddr(t)
	cfg.Governance.Quota.MaxTopics = 9
	cfg.Governance.Auth.StaticTokens = []broker.StaticAuthTokenConfig{{
		Token:     "admin-secret",
		Principal: "admin",
		Tenant:    "tenant-a",
		Namespace: "default",
		ClientID:  "admin-ui",
	}}
	cfg.Governance.TenantDefaults = []broker.TenantDefaultsConfig{{
		Tenant:            "tenant-a",
		Namespace:         "default",
		RetentionMS:       &retentionMS,
		MessageTTLMS:      &messageTTLMS,
		DelayEnabled:      &delayEnabled,
		DeadLetterTopic:   "orders.dlq",
		RetryPolicy:       store.TopicRetryPolicy{MaxAttempts: 3},
		RetentionMaxBytes: 1024,
	}}
	b, err := broker.New(cfg)
	requireNoError(t, err)
	server := broker.NewAdminServer(cfg, b, nil, nil)

	requireNoError(t, server.Start(ctx))
	defer stopAdminServer(t, server)

	status, body := getAdminResponse(t, cfg.Admin.BindAddr, "/ui/governance", map[string]string{
		"Authorization": "Bearer admin-secret",
	})
	if status != http.StatusOK {
		t.Fatalf("unexpected governance ui status: %d body=%s", status, body)
	}
	if !strings.Contains(body, "Governance") || !strings.Contains(body, "tenant-a") || !strings.Contains(body, "orders.dlq") {
		t.Fatalf("admin governance ui did not render tenant controls: %s", body)
	}
	if strings.Contains(body, "admin-secret") {
		t.Fatalf("admin governance ui leaked static token secret: %s", body)
	}
}
