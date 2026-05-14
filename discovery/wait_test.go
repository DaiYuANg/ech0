package discovery_test

import (
	"context"
	"testing"

	"github.com/lyonbrown4d/ech0/discovery"
)

func TestWaitForAliveReturnsStaticNodes(t *testing.T) {
	local := discovery.Node{ID: 1, ClusterName: "ech0", RaftAddr: "127.0.0.1:3210", Status: discovery.NodeStatusAlive}
	provider := discovery.NewStaticProvider(local, []discovery.Node{
		local,
		{ID: 2, ClusterName: "ech0", RaftAddr: "127.0.0.1:3211", Status: discovery.NodeStatusAlive},
	})

	nodes, err := discovery.WaitForAlive(context.Background(), provider, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0].ID != 1 || nodes[1].ID != 2 {
		t.Fatalf("expected nodes sorted by id, got %#v", nodes)
	}
}

func TestAliveNodesFiltersInactiveAndDedupes(t *testing.T) {
	nodes := discovery.AliveNodes([]discovery.Node{
		{ID: 2, ClusterName: "ech0", RaftAddr: "", Status: discovery.NodeStatusAlive},
		{ID: 1, ClusterName: "ech0", RaftAddr: "127.0.0.1:3210", Status: discovery.NodeStatusAlive},
		{ID: 2, ClusterName: "ech0", RaftAddr: "127.0.0.1:3211", Status: discovery.NodeStatusAlive},
		{ID: 3, ClusterName: "ech0", RaftAddr: "127.0.0.1:3212", Status: discovery.NodeStatusDead},
	})
	if len(nodes) != 2 {
		t.Fatalf("expected 2 alive nodes, got %d", len(nodes))
	}
	if nodes[0].ID != 1 || nodes[1].ID != 2 {
		t.Fatalf("expected sorted node ids 1,2 got %#v", nodes)
	}
	if nodes[1].RaftAddr != "127.0.0.1:3211" {
		t.Fatalf("expected duplicate with raft addr to win, got %q", nodes[1].RaftAddr)
	}
}
