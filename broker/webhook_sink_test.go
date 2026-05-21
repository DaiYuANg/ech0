package broker_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestWebhookSinkDeliversAndCommitsOffset(t *testing.T) {
	ctx := context.Background()
	requests := webhookSinkRequests{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		requireNoError(t, err)
		requests.add(r.Header.Get("X-Sink"), string(body))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	record := store.NewRecordAppend([]byte("m1"))
	record.Key = []byte("order-1")
	record.Headers = []store.RecordHeader{{Key: "content-type", Value: []byte("text/plain")}}
	_, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: 0}, record)
	requireNoError(t, err)

	result, err := b.ProcessWebhookSinkOnce(ctx, broker.WebhookSinkConfig{
		Name:       "orders-http",
		Topic:      "orders",
		Partition:  0,
		Consumer:   "orders-webhook",
		URL:        server.URL,
		MaxRecords: 10,
		Headers:    []broker.WebhookSinkHeaderConfig{{Key: "X-Sink", Value: "orders-http"}},
	})
	requireNoError(t, err)
	requireWebhookResult(t, result, 1, 1)
	requireWebhookCommitted(ctx, t, b)
	requireWebhookPayload(t, requests.only(t))
	requireWebhookHeader(t, &requests)
}

func TestWebhookSinkFailureLeavesOffsetUncommitted(t *testing.T) {
	ctx := context.Background()
	server := failingStatusServer(http.StatusInternalServerError)
	defer server.Close()

	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	result, err := b.ProcessWebhookSinkOnce(ctx, broker.WebhookSinkConfig{
		Name:       "orders-http",
		Topic:      "orders",
		Partition:  0,
		Consumer:   "orders-webhook",
		URL:        server.URL,
		MaxRecords: 10,
	})
	if err == nil {
		t.Fatal("expected webhook failure")
	}
	if result.Delivered != 0 || result.CommittedNextOffset != nil {
		t.Fatalf("unexpected webhook result after failure: %#v", result)
	}
	requireUncommittedConsumerOffset(ctx, t, b, "orders-webhook")
}

type webhookSinkRequests struct {
	mu          sync.Mutex
	headerValue string
	bodies      []string
}

func (r *webhookSinkRequests) add(header, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.headerValue = header
	r.bodies = append(r.bodies, body)
}

func (r *webhookSinkRequests) only(t *testing.T) string {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.bodies) != 1 {
		t.Fatalf("expected one webhook request, got %d", len(r.bodies))
	}
	return r.bodies[0]
}

func (r *webhookSinkRequests) lastHeader() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.headerValue
}

func requireWebhookResult(t *testing.T, result broker.WebhookSinkResult, delivered int, committed uint64) {
	t.Helper()
	if result.Delivered != delivered || result.CommittedNextOffset == nil || *result.CommittedNextOffset != committed {
		t.Fatalf("unexpected webhook result: %#v", result)
	}
}

func requireWebhookCommitted(ctx context.Context, t *testing.T, b *broker.Broker) {
	t.Helper()
	committed, err := b.CommittedOffset(ctx, "orders-webhook", "orders", 0)
	requireNoError(t, err)
	if committed == nil || committed.NextOffset != 1 || committed.Metadata != "webhook:orders-http" {
		t.Fatalf("unexpected committed offset: %#v", committed)
	}
}

func requireWebhookPayload(t *testing.T, body string) {
	t.Helper()
	if !strings.Contains(body, `"sink":"orders-http"`) || !strings.Contains(body, `"payload":"m1"`) || !strings.Contains(body, `"key_base64":"b3JkZXItMQ=="`) {
		t.Fatalf("unexpected webhook payload: %s", body)
	}
}

func requireWebhookHeader(t *testing.T, requests *webhookSinkRequests) {
	t.Helper()
	if requests.lastHeader() != "orders-http" {
		t.Fatalf("expected configured webhook header, got %q", requests.lastHeader())
	}
}
