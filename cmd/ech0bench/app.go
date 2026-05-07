package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	ech0 "github.com/DaiYuANg/ech0"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/logx"
)

type benchRootContext struct {
	context.Context
}

type benchmarkDataDirectory struct {
	path    string
	cleanup func()
}

type benchRunner struct {
	cfg     benchConfig
	dataDir benchmarkDataDirectory
	mq      *ech0.Broker
}

func newBenchmarkApp(rootCtx context.Context, cfg benchConfig) (*dix.App, error) {
	logger, err := newBenchmarkLogger()
	if err != nil {
		return nil, err
	}
	logx.SetDefault(logger)

	module := dix.NewModule("ech0bench",
		dix.Providers(
			dix.Value(cfg),
			dix.Value(logger),
			dix.Value(benchRootContext{Context: rootCtx}),
			dix.ProviderErr1(newBenchmarkDataDirectory),
			dix.ProviderErr3(newBenchmarkBroker),
			dix.Provider3(newBenchRunner),
		),
		dix.Hooks(
			dix.OnStop(func(_ context.Context, dataDir benchmarkDataDirectory) error {
				dataDir.Cleanup()
				return nil
			}),
			dix.OnStop(func(ctx context.Context, mq *ech0.Broker) error {
				return mq.Close(ctx)
			}),
			dix.OnStop(func(_ context.Context, logger *slog.Logger) error {
				return logx.Close(logger)
			}),
		),
	)
	app := dix.New("ech0bench", dix.UseLogger(logger), dix.Modules(module))
	if err := app.Validate(); err != nil {
		wrapped := fmt.Errorf("validate ech0bench app: %w", err)
		if closeErr := logx.Close(logger); closeErr != nil {
			return nil, errors.Join(wrapped, fmt.Errorf("close ech0bench logger after validation failure: %w", closeErr))
		}
		return nil, wrapped
	}
	return app, nil
}

func newBenchmarkLogger() (*slog.Logger, error) {
	logger, err := logx.New(
		logx.WithConsole(true),
		logx.WithErrorLevel(),
		logx.WithNoColor(),
		logx.WithCaller(false),
	)
	if err != nil {
		return nil, fmt.Errorf("create ech0bench logger: %w", err)
	}
	return logger, nil
}

func newBenchmarkDataDirectory(cfg benchConfig) (benchmarkDataDirectory, error) {
	path, cleanup, err := benchmarkDataDir(cfg.dataDir)
	if err != nil {
		return benchmarkDataDirectory{}, err
	}
	return benchmarkDataDirectory{path: path, cleanup: cleanup}, nil
}

func (d benchmarkDataDirectory) Cleanup() {
	if d.cleanup != nil {
		d.cleanup()
	}
}

func newBenchmarkBroker(root benchRootContext, cfg benchConfig, dataDir benchmarkDataDirectory) (*ech0.Broker, error) {
	mq, err := ech0.Open(root.Context, ech0.Options{
		DataDir:        dataDir.path,
		MaxFetch:       cfg.fetchBatch,
		MaxPayloadSize: cfg.payloadBytes * 2,
	})
	if err != nil {
		return nil, fmt.Errorf("open embedded broker: %w", err)
	}
	return mq, nil
}

func newBenchRunner(cfg benchConfig, dataDir benchmarkDataDirectory, mq *ech0.Broker) *benchRunner {
	return &benchRunner{cfg: cfg, dataDir: dataDir, mq: mq}
}

func (r *benchRunner) Run(ctx context.Context) error {
	if err := r.mq.CreateTopic(ctx, r.cfg.topic, ech0.Partitions(r.cfg.partitions)); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, r.cfg.duration)
	defer cancel()
	result := runWorkers(runCtx, r.mq, r.cfg)
	r.cfg.activeConsumers = result.activeConsumers
	printReport(r.cfg, r.dataDir.path, result.elapsed, result.counters, result.publish, result.fetch)
	return nil
}
