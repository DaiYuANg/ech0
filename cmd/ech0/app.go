package main

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/DaiYuANg/ech0/broker"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dix"
	"github.com/samber/oops"
	"github.com/spf13/pflag"
)

type cliConfigPath string

type cliFlagSet struct {
	flags *pflag.FlagSet
}

type cliConfigPaths struct {
	values []string
}

type cliBrokerRunner struct {
	cfg broker.Config
}

func newCLIApp(flags *pflag.FlagSet, configPath string) (*dix.App, error) {
	module := dix.NewModule("ech0-cli",
		dix.Providers(
			dix.Value(cliFlagSet{flags: flags}),
			dix.Value(cliConfigPath(configPath)),
			dix.Provider1(newCLIConfigPaths),
			dix.ProviderErr2(loadCLIBrokerConfig),
			dix.Provider1(newCLIBrokerRunner),
		),
	)
	app := dix.New("ech0-cli", dix.Modules(module))
	if err := app.Validate(); err != nil {
		return nil, oops.
			In("cli").
			Code("app_validation_failed").
			Wrapf(err, "validate cli app")
	}
	return app, nil
}

func newCLIConfigPaths(configPath cliConfigPath) cliConfigPaths {
	paths := collectionlist.NewListWithCapacity[string](1)
	if configPath != "" {
		paths.Add(string(configPath))
	}
	return cliConfigPaths{values: paths.Values()}
}

func loadCLIBrokerConfig(flags cliFlagSet, paths cliConfigPaths) (broker.Config, error) {
	cfg, err := broker.LoadConfigFromFlagSet(flags.flags, paths.values...)
	if err != nil {
		return broker.Config{}, oops.
			In("cli").
			Code("config_load_failed").
			With("config_path", strings.Join(paths.values, ",")).
			Wrapf(err, "load broker config")
	}
	return cfg, nil
}

func newCLIBrokerRunner(cfg broker.Config) *cliBrokerRunner {
	return &cliBrokerRunner{cfg: cfg}
}

func runCLIApp(ctx context.Context, app *dix.App) (err error) {
	rt, err := app.Start(ctx)
	if err != nil {
		return oops.
			In("cli").
			Code("app_start_failed").
			Wrapf(err, "start cli app")
	}
	defer func() {
		err = errors.Join(err, stopCLIRuntime(ctx, rt))
	}()

	runner, err := dix.ResolveAs[*cliBrokerRunner](rt.Container())
	if err != nil {
		return oops.
			In("cli").
			Code("runner_resolve_failed").
			Wrapf(err, "resolve broker runner")
	}
	return runner.Run(ctx)
}

func (r *cliBrokerRunner) Run(ctx context.Context) error {
	if err := broker.RunWithConfig(ctx, r.cfg); err != nil {
		return oops.
			In("cli").
			Code("broker_run_failed").
			With("node_id", r.cfg.Broker.NodeID, "raft_engine", "dragonboat").
			Wrapf(err, "run broker")
	}
	return nil
}

func stopCLIRuntime(ctx context.Context, rt *dix.Runtime) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := rt.Stop(stopCtx); err != nil {
		return oops.
			In("cli").
			Code("app_stop_failed").
			Wrapf(err, "stop cli app")
	}
	return nil
}
