package broker_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/arcgolabs/dix"
	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestAdminUILoadsEmbeddedTailwindTemplates(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Admin.Enabled = true
	cfg.Admin.BindAddr = freeTCPAddr(t)
	b := newTestBroker(t)
	server := broker.NewAdminServer(cfg, b, nil, nil)

	requireNoError(t, server.Start(ctx))
	defer stopAdminServer(t, server)

	body := getAdminPage(t, cfg.Admin.BindAddr, "/ui")
	if !strings.Contains(body, "Broker Dashboard") || !strings.Contains(body, "https://cdn.tailwindcss.com") {
		t.Fatalf("admin ui did not render embedded Tailwind template: %s", body)
	}
}

func TestAdminRuntimeEventsDebugEndpoint(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Admin.Enabled = true
	cfg.Admin.DebugEnabled = true
	cfg.Admin.BindAddr = freeTCPAddr(t)
	recorder := dix.NewEventRecorder(4)
	recorder.LogEvent(ctx, dix.StartEvent{})
	b := newTestBroker(t)
	server := broker.NewAdminServer(cfg, b, nil, nil, recorder)

	requireNoError(t, server.Start(ctx))
	defer stopAdminServer(t, server)

	body := getAdminPage(t, cfg.Admin.BindAddr, "/api/runtime/events")
	if !strings.Contains(body, "StartEvent") {
		t.Fatalf("admin runtime events endpoint did not return recorder events: %s", body)
	}
}

func TestAdminRuntimeEventsSSEDebugEndpoint(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Admin.Enabled = true
	cfg.Admin.DebugEnabled = true
	cfg.Admin.BindAddr = freeTCPAddr(t)
	recorder := dix.NewEventRecorder(4)
	recorder.LogEvent(ctx, dix.StartEvent{})
	b := newTestBroker(t)
	server := broker.NewAdminServer(cfg, b, nil, nil, recorder)

	requireNoError(t, server.Start(ctx))
	defer stopAdminServer(t, server)

	body := getAdminStreamChunk(t, cfg.Admin.BindAddr, "/api/runtime/events/stream")
	if !strings.Contains(body, "event: runtime_events") || !strings.Contains(body, "StartEvent") {
		t.Fatalf("admin runtime events stream did not return recorder events: %s", body)
	}
}

func TestAdminTopicsRespectTenantQuery(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Admin.Enabled = true
	cfg.Admin.BindAddr = freeTCPAddr(t)
	b := newTestBroker(t)
	createTopic(tenantContext("tenant-a"), t, b, store.NewTopicConfig("orders-a"))
	createTopic(tenantContext("tenant-b"), t, b, store.NewTopicConfig("orders-b"))
	server := broker.NewAdminServer(cfg, b, nil, nil)

	requireNoError(t, server.Start(ctx))
	defer stopAdminServer(t, server)

	body := getAdminPage(t, cfg.Admin.BindAddr, "/api/topics?tenant=tenant-a&namespace=default&principal=admin")
	if !strings.Contains(body, "orders-a") || strings.Contains(body, "orders-b") {
		t.Fatalf("admin topics api did not respect tenant query: %s", body)
	}
	body = getAdminPage(t, cfg.Admin.BindAddr, "/ui/topics?tenant=tenant-b&namespace=default&principal=admin")
	if !strings.Contains(body, "orders-b") || strings.Contains(body, "orders-a") {
		t.Fatalf("admin topics ui did not respect tenant query: %s", body)
	}
}

func TestAdminACLPoliciesUIAndAPI(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Admin.Enabled = true
	cfg.Admin.BindAddr = freeTCPAddr(t)
	b := newTestBroker(t)
	policy, err := b.UpsertACLPolicy(context.Background(), broker.ACLPolicy{
		Tenant:       "tenant-a",
		Namespace:    "default",
		Principal:    "svc-a",
		ResourceType: broker.ACLResourceTopic,
		ResourceName: "orders",
		Actions:      []broker.ACLAction{broker.ACLActionProduce},
		Effect:       broker.ACLPolicyEffectAllow,
	})
	requireNoError(t, err)
	server := broker.NewAdminServer(cfg, b, nil, nil)

	requireNoError(t, server.Start(ctx))
	defer stopAdminServer(t, server)

	body := getAdminPage(t, cfg.Admin.BindAddr, "/api/acl/policies")
	if !strings.Contains(body, policy.PolicyID) || !strings.Contains(body, "svc-a") {
		t.Fatalf("admin acl api did not return policy: %s", body)
	}
	body = getAdminPage(t, cfg.Admin.BindAddr, "/ui/acls")
	if !strings.Contains(body, "ACL Policies") || !strings.Contains(body, policy.PolicyID) {
		t.Fatalf("admin acl ui did not render policy: %s", body)
	}
}

func getAdminPage(t *testing.T, addr, path string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, http.NoBody)
	requireNoError(t, err)
	response, err := http.DefaultClient.Do(request)
	requireNoError(t, err)
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Logf("close admin response body: %v", closeErr)
		}
	}()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected admin status: %d", response.StatusCode)
	}
	body, err := io.ReadAll(response.Body)
	requireNoError(t, err)
	return string(body)
}

func getAdminStreamChunk(t *testing.T, addr, path string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, http.NoBody)
	requireNoError(t, err)
	response, err := http.DefaultClient.Do(request)
	requireNoError(t, err)
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Logf("close admin stream response body: %v", closeErr)
		}
	}()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected admin stream status: %d", response.StatusCode)
	}
	if !strings.Contains(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("unexpected admin stream content type: %s", response.Header.Get("Content-Type"))
	}
	buf := make([]byte, 2048)
	n, err := response.Body.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read admin stream chunk: %v", err)
	}
	return string(buf[:n])
}

func stopAdminServer(t *testing.T, server *broker.AdminServer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requireNoError(t, server.Stop(ctx))
}
