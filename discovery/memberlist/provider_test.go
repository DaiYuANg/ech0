package memberlist_test

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/lyonbrown4d/ech0/discovery"
	"github.com/lyonbrown4d/ech0/discovery/memberlist"
)

func TestMemberlistProviderConvergesAndKeepsMetadataStable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	addr1 := freeMemberlistAddr(t)
	addr2 := freeMemberlistAddr(t)
	node1 := discovery.Node{
		ID:             1,
		ClusterName:    "ech0-memberlist-test",
		RaftAddr:       "127.0.0.1:6201",
		BrokerAddr:     "127.0.0.1:7201",
		AdminAddr:      "127.0.0.1:8201",
		DataShardCount: 4,
		Tags:           map[string]string{"rack": "a"},
	}
	node2 := discovery.Node{
		ID:             2,
		ClusterName:    "ech0-memberlist-test",
		RaftAddr:       "127.0.0.1:6202",
		BrokerAddr:     "127.0.0.1:7202",
		AdminAddr:      "127.0.0.1:8202",
		DataShardCount: 4,
		Tags:           map[string]string{"rack": "b"},
	}
	provider1 := startMemberlistProvider(ctx, t, node1, addr1, nil)
	provider2 := startMemberlistProvider(ctx, t, node2, addr2, []string{addr1})

	nodes1, err := discovery.WaitForAlive(ctx, provider1, 2, 25*time.Millisecond)
	requireNoError(t, err)
	nodes2, err := discovery.WaitForAlive(ctx, provider2, 2, 25*time.Millisecond)
	requireNoError(t, err)
	assertMemberlistNodes(t, nodes1)
	assertMemberlistNodes(t, nodes2)

	for range 5 {
		assertMemberlistNodes(t, discovery.AliveNodes(provider1.Nodes()))
		assertMemberlistNodes(t, discovery.AliveNodes(provider2.Nodes()))
		time.Sleep(20 * time.Millisecond)
	}
}

func startMemberlistProvider(
	ctx context.Context,
	t *testing.T,
	node discovery.Node,
	bindAddr string,
	seeds []string,
) *memberlist.Provider {
	t.Helper()
	provider, err := memberlist.NewProvider(memberlist.Config{
		LocalNode:   node,
		BindAddr:    bindAddr,
		Seeds:       seeds,
		JoinTimeout: 3 * time.Second,
		Logger:      slog.New(slog.DiscardHandler),
	})
	requireNoError(t, err)
	requireNoError(t, provider.Start(ctx))
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		requireNoError(t, provider.Stop(stopCtx))
	})
	return provider
}

func assertMemberlistNodes(t *testing.T, nodes []discovery.Node) {
	t.Helper()
	if len(nodes) != 2 {
		t.Fatalf("expected 2 alive nodes, got %#v", nodes)
	}
	if nodes[0].ID != 1 || nodes[1].ID != 2 {
		t.Fatalf("expected sorted node ids 1,2 got %#v", nodes)
	}
	assertMemberlistNodeMetadata(t, nodes[0], 1, "127.0.0.1:6201", "a")
	assertMemberlistNodeMetadata(t, nodes[1], 2, "127.0.0.1:6202", "b")
}

func assertMemberlistNodeMetadata(t *testing.T, node discovery.Node, id uint64, raftAddr, rack string) {
	t.Helper()
	if node.ID != id {
		t.Fatalf("unexpected node id: %#v", node)
	}
	if node.ClusterName != "ech0-memberlist-test" {
		t.Fatalf("unexpected cluster name: %#v", node)
	}
	if node.RaftAddr != raftAddr {
		t.Fatalf("unexpected raft address: %#v", node)
	}
	if node.BrokerAddr == "" || node.AdminAddr == "" || node.DataShardCount != 4 {
		t.Fatalf("metadata did not converge: %#v", node)
	}
	if node.Tags["rack"] != rack {
		t.Fatalf("tags did not converge: %#v", node)
	}
}

func freeMemberlistAddr(t *testing.T) string {
	t.Helper()
	listenConfig := net.ListenConfig{}
	for range 50 {
		packet, err := listenConfig.ListenPacket(context.Background(), "udp", "127.0.0.1:0")
		if err != nil {
			continue
		}
		addr := packet.LocalAddr().String()
		requireNoError(t, packet.Close())
		listener, err := listenConfig.Listen(context.Background(), "tcp", addr)
		if err != nil {
			continue
		}
		requireNoError(t, listener.Close())
		return addr
	}
	t.Fatal("find free memberlist tcp/udp address")
	return ""
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
