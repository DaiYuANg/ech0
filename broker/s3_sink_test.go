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

func TestS3SinkPutsObjectAndCommitsOffset(t *testing.T) {
	ctx := context.Background()
	requests := s3SinkRequests{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		requireNoError(t, err)
		requests.add(r.URL.Path, r.Header.Get("Authorization"), r.Header.Get("X-Amz-Content-Sha256"), string(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	result, err := b.ProcessS3SinkOnce(ctx, broker.S3SinkConfig{
		Name:            "orders-s3",
		Topic:           "orders",
		Partition:       0,
		Consumer:        "orders-s3",
		EndpointURL:     server.URL,
		Bucket:          "archive",
		Prefix:          "mirror",
		AccessKeyID:     "test-access",
		SecretAccessKey: strings.Repeat("x", 16),
		MaxRecords:      10,
	})
	requireNoError(t, err)
	requireS3SinkResult(t, result, 1, 1)
	requireS3SinkCommitted(ctx, t, b)
	request := requests.only(t)
	if request.path != "/archive/mirror/orders/0/0.json" {
		t.Fatalf("unexpected s3 object path: %s", request.path)
	}
	if !strings.HasPrefix(request.auth, "AWS4-HMAC-SHA256 ") || request.payloadHash == "" {
		t.Fatalf("expected signed s3 request, got auth=%q payloadHash=%q", request.auth, request.payloadHash)
	}
	if !strings.Contains(request.body, `"sink":"orders-s3"`) || !strings.Contains(request.body, `"payload":"m1"`) {
		t.Fatalf("unexpected s3 body: %s", request.body)
	}
}

func TestS3SinkFailureLeavesOffsetUncommitted(t *testing.T) {
	ctx := context.Background()
	server := failingStatusServer(http.StatusServiceUnavailable)
	defer server.Close()

	b := newTestBroker(t)
	createTopic(ctx, t, b, store.NewTopicConfig("orders"))
	publishOrder(ctx, t, b, []byte("m1"))

	result, err := b.ProcessS3SinkOnce(ctx, broker.S3SinkConfig{
		Name:        "orders-s3",
		Topic:       "orders",
		Partition:   0,
		Consumer:    "orders-s3",
		EndpointURL: server.URL,
		Bucket:      "archive",
		MaxRecords:  10,
	})
	if err == nil {
		t.Fatal("expected s3 sink failure")
	}
	if result.Delivered != 0 || result.CommittedNextOffset != nil {
		t.Fatalf("unexpected s3 result after failure: %#v", result)
	}
	requireUncommittedConsumerOffset(ctx, t, b, "orders-s3")
}

type s3Request struct {
	path        string
	auth        string
	payloadHash string
	body        string
}

type s3SinkRequests struct {
	mu       sync.Mutex
	requests []s3Request
}

func (r *s3SinkRequests) add(path, auth, payloadHash, body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests = append(r.requests, s3Request{path: path, auth: auth, payloadHash: payloadHash, body: body})
}

func (r *s3SinkRequests) only(t *testing.T) s3Request {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.requests) != 1 {
		t.Fatalf("expected one s3 request, got %d", len(r.requests))
	}
	return r.requests[0]
}

func requireS3SinkResult(t *testing.T, result broker.S3SinkResult, delivered int, committed uint64) {
	t.Helper()
	if result.Delivered != delivered || result.CommittedNextOffset == nil || *result.CommittedNextOffset != committed {
		t.Fatalf("unexpected s3 sink result: %#v", result)
	}
}

func requireS3SinkCommitted(ctx context.Context, t *testing.T, b *broker.Broker) {
	t.Helper()
	committed, err := b.CommittedOffset(ctx, "orders-s3", "orders", 0)
	requireNoError(t, err)
	if committed == nil || committed.NextOffset != 1 || committed.Metadata != "s3:orders-s3" {
		t.Fatalf("unexpected committed offset: %#v", committed)
	}
}
