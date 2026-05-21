package broker_test

import (
	"path/filepath"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestConfigShardPathsUseTargetLayout(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = filepath.Join("var", "lib", "ech0")
	cfg.Broker.NodeID = 7

	if got, want := cfg.DragonboatDir(), filepath.Join("var", "lib", "ech0", "dragonboat", "7"); got != want {
		t.Fatalf("dragonboat dir = %q, want %q", got, want)
	}
	if got, want := cfg.ShardDir(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012"); got != want {
		t.Fatalf("shard dir = %q, want %q", got, want)
	}
	if got, want := cfg.ShardSegmentLogPath(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012", "segments"); got != want {
		t.Fatalf("shard segment path = %q, want %q", got, want)
	}
}

func TestBrokerRuntimeHealthIncludesConfiguredDataShards(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.DataShardCount = 3

	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	health := b.RuntimeHealth()
	if health.RuntimeMode != "single_replica_cluster" {
		t.Fatalf("runtime mode = %q, want single_replica_cluster", health.RuntimeMode)
	}
	if len(health.DataShards) != 3 {
		t.Fatalf("data shard health count = %d, want 3: %#v", len(health.DataShards), health.DataShards)
	}
	for index, shard := range health.DataShards {
		if shard.ShardID != store.ShardID(index) {
			t.Fatalf("data shard health[%d] shard_id = %d", index, shard.ShardID)
		}
		if shard.RuntimeMode != "local_segment" {
			t.Fatalf("data shard health[%d] runtime_mode = %q", index, shard.RuntimeMode)
		}
	}
}

func TestBrokerRuntimeHealthUsesDragonboatGroupsForMultiPeerDataShards(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.DataShardCount = 2
	cfg.Raft.Cluster = []broker.RaftPeerConfig{
		{NodeID: 1, Addr: "127.0.0.1:3210"},
		{NodeID: 2, Addr: "127.0.0.1:3211"},
	}

	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	health := b.RuntimeHealth()
	if health.RuntimeMode != "cluster" {
		t.Fatalf("runtime mode = %q, want cluster", health.RuntimeMode)
	}
	for index, shard := range health.DataShards {
		if shard.ShardID != store.ShardID(index) {
			t.Fatalf("data shard health[%d] shard_id = %d", index, shard.ShardID)
		}
		if shard.RuntimeMode != "dragonboat_group" {
			t.Fatalf("data shard health[%d] runtime_mode = %q", index, shard.RuntimeMode)
		}
	}
}

func TestBrokerClusterMetadataSummarizesConfiguredPeers(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.NodeID = 2
	cfg.Broker.ClusterName = "ech0-test"
	cfg.Raft.BindAddr = "0.0.0.0:6302"
	cfg.Raft.AdvertiseAddr = ""
	cfg.Raft.Cluster = []broker.RaftPeerConfig{
		{NodeID: 2, Addr: "127.0.0.1:6302"},
		{NodeID: 1, Addr: "127.0.0.1:6301"},
	}
	b, err := broker.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	metadata := b.ClusterMetadata()
	if metadata.ClusterName != "ech0-test" || metadata.NodeID != 2 {
		t.Fatalf("unexpected cluster identity: %#v", metadata)
	}
	if metadata.Raft.AdvertiseAddr != "127.0.0.1:6302" || metadata.Raft.KnownNodes != 2 {
		t.Fatalf("unexpected raft metadata: %#v", metadata.Raft)
	}
	if len(metadata.Raft.Peers) != 2 || metadata.Raft.Peers[0].NodeID != 1 || metadata.Raft.Peers[1].NodeID != 2 || !metadata.Raft.Peers[1].Local {
		t.Fatalf("unexpected peer metadata: %#v", metadata.Raft.Peers)
	}
}
