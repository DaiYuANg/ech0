package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/DaiYuANg/ech0/broker"
	"github.com/spf13/pflag"
)

func main() {
	var configPath string

	flags := pflag.NewFlagSet("ech0", pflag.ExitOnError)
	flags.StringVar(&configPath, "config", "", "config file path")
	flags.String("broker-addr", "", "tcp broker bind address")
	flags.String("admin-addr", "", "admin http bind address")
	flags.String("data-dir", "", "broker data directory")
	flags.Bool("raft", false, "enable raft mode")
	if err := flags.Parse(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	var paths []string
	if configPath != "" {
		paths = append(paths, configPath)
	}
	cfg, err := broker.LoadConfigFromFlagSet(flags, paths...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := broker.RunWithConfig(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
