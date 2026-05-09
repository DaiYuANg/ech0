package broker_test

import (
	"path/filepath"
	"testing"

	broker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
)

func TestConfigShardPathsUseTargetLayout(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = filepath.Join("var", "lib", "ech0")
	cfg.Broker.NodeID = 7

	if got, want := cfg.MetadataDir(), filepath.Join("var", "lib", "ech0", "metadata"); got != want {
		t.Fatalf("metadata dir = %q, want %q", got, want)
	}
	if got, want := cfg.MetadataRaftDir(), filepath.Join("var", "lib", "ech0", "metadata", "raft", "7"); got != want {
		t.Fatalf("metadata raft dir = %q, want %q", got, want)
	}
	if got, want := cfg.ShardDir(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012"); got != want {
		t.Fatalf("shard dir = %q, want %q", got, want)
	}
	if got, want := cfg.ShardRaftDir(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012", "raft", "7"); got != want {
		t.Fatalf("shard raft dir = %q, want %q", got, want)
	}
	if got, want := cfg.ShardSegmentLogPath(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012", "segments"); got != want {
		t.Fatalf("shard segment path = %q, want %q", got, want)
	}
	if got, want := cfg.ShardBadgerPath(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012", "badger"); got != want {
		t.Fatalf("shard badger path = %q, want %q", got, want)
	}
}
