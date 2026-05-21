package broker_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestAdminHTTPGatewayProducesFetchesAndCommits(t *testing.T) {
	ctx := context.Background()
	cfg := broker.DefaultConfig()
	cfg.Admin.Enabled = true
	cfg.Admin.BindAddr = freeTCPAddr(t)
	b := newTestBroker(t)
	topic := store.NewTopicConfig("orders")
	topic.PriorityPolicy = store.TopicPriorityPolicy{Enabled: true, Min: 0, Max: 9, Default: 0}
	createTopic(ctx, t, b, topic)
	server := broker.NewAdminServer(cfg, b, nil, nil)

	requireNoError(t, server.Start(ctx))
	defer stopAdminServer(t, server)

	status, body := postAdminJSON(t, cfg.Admin.BindAddr, "/api/gateway/topics/orders/records", `{
		"payload": "created",
		"key": "order-1",
		"priority": 7,
		"headers": [{"key":"content-type","value":"text/plain"}]
	}`)
	if status != http.StatusOK || !strings.Contains(body, `"offset":0`) {
		t.Fatalf("gateway produce failed status=%d body=%s", status, body)
	}

	body = getAdminPage(t, cfg.Admin.BindAddr, "/api/gateway/topics/orders/partitions/0/records?consumer=gateway&max_records=10")
	if !strings.Contains(body, `"payload":"created"`) || !strings.Contains(body, `"x-ech0-priority"`) {
		t.Fatalf("gateway fetch did not return produced message: %s", body)
	}

	status, body = postAdminJSON(t, cfg.Admin.BindAddr, "/api/gateway/topics/orders/partitions/0/commit", `{
		"consumer": "gateway",
		"next_offset": 1,
		"metadata": "http-gateway"
	}`)
	if status != http.StatusOK || !strings.Contains(body, `"next_offset":1`) {
		t.Fatalf("gateway commit failed status=%d body=%s", status, body)
	}
}

func postAdminJSON(t *testing.T, addr, path, body string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+path, bytes.NewBufferString(body))
	requireNoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	requireNoError(t, err)
	defer func() {
		if closeErr := response.Body.Close(); closeErr != nil {
			t.Logf("close admin response body: %v", closeErr)
		}
	}()
	raw, err := io.ReadAll(response.Body)
	requireNoError(t, err)
	return response.StatusCode, string(raw)
}
