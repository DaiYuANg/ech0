package broker_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arcgolabs/dix"
	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/spf13/pflag"
)

func TestLoadConfigFromFlagSetUsesConfigxSources(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ech0.toml")
	if err := os.WriteFile(path, []byte(`
[broker]
node_id = 7
data_dir = "./from-file"
bind_addr = "127.0.0.1:19090"

[admin]
bind_addr = "127.0.0.1:19091"
`), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ECH0_BROKER__DATA_DIR", "./from-env")

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("broker-addr", "", "")
	if err := flags.Parse([]string{"--broker-addr=127.0.0.1:29090"}); err != nil {
		t.Fatal(err)
	}

	cfg, err := broker.LoadConfigFromFlagSet(flags, path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Broker.NodeID != 7 {
		t.Fatalf("expected file node id, got %d", cfg.Broker.NodeID)
	}
	if cfg.Broker.DataDir != "./from-env" {
		t.Fatalf("expected env data dir, got %q", cfg.Broker.DataDir)
	}
	if cfg.Broker.BindAddr != "127.0.0.1:29090" {
		t.Fatalf("expected flag broker addr, got %q", cfg.Broker.BindAddr)
	}
	if cfg.Raft.BindAddr != "127.0.0.1:3210" {
		t.Fatalf("expected default raft bind addr, got %q", cfg.Raft.BindAddr)
	}
}

func TestNewAppFromConfigSourceLoadsConfigWithDIX(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "ech0.toml")
	dataDir := filepath.ToSlash(filepath.Join(root, "data"))
	raftAddr := freeTCPAddr(t)
	content := fmt.Appendf(nil, `
[broker]
node_id = 9
data_dir = %q
bind_addr = "127.0.0.1:0"

[admin]
enabled = false

[raft]
bind_addr = %q

[[raft.cluster]]
node_id = 9
addr = %q
`, dataDir, raftAddr, raftAddr)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}

	app, err := broker.NewAppFromConfigSource(broker.NewConfigSource(path))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rt, err := app.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if stopErr := rt.Stop(ctx); stopErr != nil {
			t.Fatal(stopErr)
		}
	}()

	cfg, err := dix.ResolveAs[broker.Config](rt.Container())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Broker.NodeID != 9 {
		t.Fatalf("expected dix-loaded node id, got %d", cfg.Broker.NodeID)
	}
	if cfg.Broker.DataDir != dataDir {
		t.Fatalf("expected dix-loaded data dir, got %q", cfg.Broker.DataDir)
	}
}
