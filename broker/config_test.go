package broker_test

import (
	"os"
	"path/filepath"
	"testing"

	broker "github.com/DaiYuANg/ech0/broker"
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
