package admin_test

import (
	"context"
	"net"
	"testing"

	ech0 "github.com/lyonbrown4d/ech0"
	"github.com/lyonbrown4d/ech0/admin"
)

func TestAdminClientCreatesAndReadsTopicViews(t *testing.T) {
	ctx := context.Background()
	b := openBroker(ctx, t)
	defer closeBroker(ctx, t, b)

	client, err := admin.New(b)
	requireNoError(t, err)
	requireNoError(t, client.CreateTopic(ctx, "orders", ech0.Partitions(2)))

	topics, err := client.Topics(ctx)
	requireNoError(t, err)
	if len(topics) != 1 || topics[0].Name != "orders" || topics[0].Partitions != 2 {
		t.Fatalf("unexpected topics: %#v", topics)
	}
	if health := client.Health(); health.Status == "" || health.RuntimeMode == "" {
		t.Fatalf("unexpected health: %#v", health)
	}
	assertClusterMetadata(t, client.Cluster())
	quota, err := client.Quota(ctx)
	requireNoError(t, err)
	if quota.Identity.Tenant == "" || quota.Identity.Namespace == "" {
		t.Fatalf("unexpected quota: %#v", quota)
	}
	metrics, err := client.StreamMetrics(ctx)
	requireNoError(t, err)
	if metrics.TopicCount != 1 {
		t.Fatalf("unexpected stream metrics: %#v", metrics)
	}
}

func assertClusterMetadata(t *testing.T, cluster admin.ClusterMetadata) {
	t.Helper()
	if cluster.Raft.Engine != "dragonboat" || cluster.NodeID == 0 {
		t.Fatalf("unexpected cluster metadata: %#v", cluster)
	}
}

func openBroker(ctx context.Context, t *testing.T) *ech0.Broker {
	t.Helper()
	opts := ech0.DefaultOptions()
	opts.DataDir = t.TempDir()
	opts.DisableDelay = true
	opts.DisableRetry = true
	addr := freeAddr(ctx, t)
	opts.Raft = &ech0.RaftOptions{
		BindAddr: addr,
		Peers:    []ech0.RaftPeer{{NodeID: opts.NodeID, Addr: addr}},
	}
	b, err := ech0.Open(ctx, opts)
	requireNoError(t, err)
	return b
}

func freeAddr(ctx context.Context, t *testing.T) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	requireNoError(t, err)
	defer func() {
		requireNoError(t, listener.Close())
	}()
	return listener.Addr().String()
}

func closeBroker(ctx context.Context, t *testing.T, b *ech0.Broker) {
	t.Helper()
	requireNoError(t, b.Close(ctx))
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
