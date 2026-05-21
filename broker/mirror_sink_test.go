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

func TestMirrorSinkReplicatesThroughHTTPGatewayAndCommitsOffset(t *testing.T) {
	ctx := context.Background()
	requests := mirrorSinkRequests{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		requireNoError(t, err)
		requests.add(r.URL.Path, r.Header.Get("Authorization"), string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	result, err := b.ProcessMirrorSinkOnce(ctx, broker.MirrorSinkConfig{
		Name:           "orders-mirror",
		Topic:          "orders",
		TargetTopic:    "orders-copy",
		Partition:      0,
		Consumer:       "orders-mirror",
		TargetAdminURL: server.URL,
		AuthToken:      "remote-token",
		MaxRecords:     10,
	})
	requireNoError(t, err)
	requireMirrorSinkResult(t, result, 1, 1)
	requireMirrorSinkCommitted(ctx, t, b)
	request := requests.only(t)
	if request.path != "/api/gateway/topics/orders-copy/records" || request.auth != "Bearer remote-token" {
		t.Fatalf("unexpected mirror request metadata: %#v", request)
	}
	if !strings.Contains(request.body, `"payload":"m1"`) || !strings.Contains(request.body, `"x-ech0-mirror-source-offset"`) {
		t.Fatalf("unexpected mirror request body: %s", request.body)
	}
}

func TestMirrorSinkFailureLeavesOffsetUncommitted(t *testing.T) {
	ctx := context.Background()
	server := failingStatusServer(http.StatusBadGateway)
	defer server.Close()

	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	result, err := b.ProcessMirrorSinkOnce(ctx, broker.MirrorSinkConfig{
		Name:           "orders-mirror",
		Topic:          "orders",
		Partition:      0,
		Consumer:       "orders-mirror",
		TargetAdminURL: server.URL,
		MaxRecords:     10,
	})
	if err == nil {
		t.Fatal("expected mirror failure")
	}
	if result.Delivered != 0 || result.CommittedNextOffset != nil {
		t.Fatalf("unexpected mirror result after failure: %#v", result)
	}
	requireUncommittedConsumerOffset(ctx, t, b, "orders-mirror")
}

type mirrorRequest struct {
	path string
	auth string
	body string
}

type mirrorSinkRequests struct {
	mu       sync.Mutex
	requests []mirrorRequest
}

func (r *mirrorSinkRequests) add(path, auth, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, mirrorRequest{path: path, auth: auth, body: body})
}

func (r *mirrorSinkRequests) only(t *testing.T) mirrorRequest {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.requests) != 1 {
		t.Fatalf("expected one mirror request, got %d", len(r.requests))
	}
	return r.requests[0]
}

func requireMirrorSinkResult(t *testing.T, result broker.MirrorSinkResult, delivered int, committed uint64) {
	t.Helper()
	if result.Delivered != delivered || result.CommittedNextOffset == nil || *result.CommittedNextOffset != committed {
		t.Fatalf("unexpected mirror sink result: %#v", result)
	}
}

func requireMirrorSinkCommitted(ctx context.Context, t *testing.T, b *broker.Broker) {
	t.Helper()
	committed, err := b.CommittedOffset(ctx, "orders-mirror", "orders", 0)
	requireNoError(t, err)
	if committed == nil || committed.NextOffset != 1 || committed.Metadata != "mirror:orders-mirror" {
		t.Fatalf("unexpected committed offset: %#v", committed)
	}
}
