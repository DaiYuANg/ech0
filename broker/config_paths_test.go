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
	if got, want := cfg.DragonboatDir(), filepath.Join("var", "lib", "ech0", "dragonboat", "7"); got != want {
		t.Fatalf("dragonboat dir = %q, want %q", got, want)
	}
	if got, want := cfg.ShardDir(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012"); got != want {
		t.Fatalf("shard dir = %q, want %q", got, want)
	}
	if got, want := cfg.ShardSegmentLogPath(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012", "segments"); got != want {
		t.Fatalf("shard segment path = %q, want %q", got, want)
	}
	if got, want := cfg.ShardBadgerPath(store.ShardID(12)), filepath.Join("var", "lib", "ech0", "shards", "shard-0012", "badger"); got != want {
		t.Fatalf("shard badger path = %q, want %q", got, want)
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
	if len(health.DataShards) != 3 {
		t.Fatalf("data shard health count = %d, want 3: %#v", len(health.DataShards), health.DataShards)
	}
	for index, shard := range health.DataShards {
		if shard.ShardID != store.ShardID(index) {
			t.Fatalf("data shard health[%d] shard_id = %d", index, shard.ShardID)
		}
		if shard.RuntimeMode != "standalone" {
			t.Fatalf("data shard health[%d] runtime_mode = %q", index, shard.RuntimeMode)
		}
	}
}
