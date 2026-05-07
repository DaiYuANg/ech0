// Package main provides the ech0 command-line broker.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/DaiYuANg/ech0/broker"
	"github.com/samber/oops"
	"github.com/spf13/cobra"
)

func main() {
	if err := newRootCommand().Execute(); err != nil {
		cobra.CheckErr(err)
	}
}

func newRootCommand() *cobra.Command {
	var configPath string

	root := &cobra.Command{
		Use:           "ech0",
		Short:         "Run an embedded ech0 message broker",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runBroker(cmd.Context(), cmd, configPath)
		},
	}

	flags := root.Flags()
	flags.StringVar(&configPath, "config", "", "config file path")
	flags.String("broker-addr", "", "tcp broker bind address")
	flags.String("admin-addr", "", "admin http bind address")
	flags.String("data-dir", "", "broker data directory")
	flags.Bool("raft", false, "enable raft mode")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	root.SetContext(ctx)
	root.SetHelpCommand(&cobra.Command{Hidden: true})
	cobra.OnFinalize(stop)

	return root
}

func runBroker(ctx context.Context, cmd *cobra.Command, configPath string) error {
	var paths []string
	if configPath != "" {
		paths = append(paths, configPath)
	}
	cfg, err := broker.LoadConfigFromFlagSet(cmd.Flags(), paths...)
	if err != nil {
		return oops.
			In("cli").
			Code("config_load_failed").
			With("config_path", configPath).
			Wrapf(err, "load broker config")
	}

	if err := broker.RunWithConfig(ctx, cfg); err != nil {
		return oops.
			In("cli").
			Code("broker_run_failed").
			With("node_id", cfg.Broker.NodeID, "raft_enabled", cfg.Raft.Enabled).
			Wrapf(err, "run broker")
	}
	return nil
}
