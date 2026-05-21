package broker_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
)

func failingStatusServer(status int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
}

func requireUncommittedConsumerOffset(ctx context.Context, t *testing.T, b *broker.Broker, consumer string) {
	t.Helper()
	committed, err := b.CommittedOffset(ctx, consumer, "orders", 0)
	requireNoError(t, err)
	if committed != nil {
		t.Fatalf("expected no committed offset for %s, got %#v", consumer, committed)
	}
}
