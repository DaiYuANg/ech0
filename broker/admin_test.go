package broker_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	broker "github.com/DaiYuANg/ech0/broker"
	"github.com/arcgolabs/dix"
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

func stopAdminServer(t *testing.T, server *broker.AdminServer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requireNoError(t, server.Stop(ctx))
}
