// Package main provides the ech0 command-line broker.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/samber/oops"
	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
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
		Version:       version,
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	root.SetContext(ctx)
	root.SetHelpCommand(&cobra.Command{Hidden: true})
	root.AddCommand(newVersionCommand())
	root.AddCommand(newRepairCommand())
	cobra.OnFinalize(stop)

	return root
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print build version information",
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "ech0 %s\ncommit: %s\nbuilt: %s\n", version, commit, date)
			return oops.In("cli").Code("version_write_failed").Wrapf(err, "write version")
		},
	}
}

func runBroker(ctx context.Context, cmd *cobra.Command, configPath string) error {
	app, err := newCLIApp(cmd.Flags(), configPath)
	if err != nil {
		return err
	}
	return runCLIApp(ctx, app)
}
