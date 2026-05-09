package main

import (
	"context"
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
	mq      benchBroker
}

func newBenchmarkApp(rootCtx context.Context, cfg benchConfig) (*dix.App, error) {
	module := dix.NewModule("ech0bench",
		dix.Providers(
			dix.Value(cfg),
			dix.Value(benchRootContext{Context: rootCtx}),
			dix.ProviderErr0(newBenchmarkLogger, dix.Eager()),
			dix.ProviderErr1(newBenchmarkDataDirectory),
			dix.ProviderErr4(newBenchmarkBroker),
			dix.Provider3(newBenchRunner),
		),
		dix.Hooks(
			dix.OnStop(func(_ context.Context, dataDir benchmarkDataDirectory) error {
				dataDir.Cleanup()
				return nil
			}),
			dix.OnStop(func(ctx context.Context, mq benchBroker) error {
				return mq.Close(ctx)
			}),
			dix.OnStop(func(_ context.Context, logger *slog.Logger) error {
				return logx.Close(logger)
			}),
		),
	)
	app := dix.New("ech0bench", dix.Modules(module))
	if err := app.Validate(); err != nil {
		return nil, fmt.Errorf("validate ech0bench app: %w", err)
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
	logx.SetDefault(logger)
	return logger, nil
}

func newBenchmarkDataDirectory(cfg benchConfig) (benchmarkDataDirectory, error) {
	if cfg.brokerAddr != "" && cfg.dataDir == "" {
		return benchmarkDataDirectory{}, nil
	}
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

func newBenchmarkBroker(
	root benchRootContext,
	cfg benchConfig,
	dataDir benchmarkDataDirectory,
	logger *slog.Logger,
) (benchBroker, error) {
	if cfg.brokerAddr != "" {
		return newTCPBenchBroker(cfg), nil
	}
	mq, err := ech0.Open(root.Context, ech0.Options{
		DataDir:        dataDir.path,
		MaxFetch:       cfg.fetchBatch,
		MaxPayloadSize: cfg.payloadBytes * 2,
		Logger:         logger,
	})
	if err != nil {
		return nil, fmt.Errorf("open embedded broker: %w", err)
	}
	return &embeddedBenchBroker{mq: mq, cfg: cfg}, nil
}

func newBenchRunner(cfg benchConfig, dataDir benchmarkDataDirectory, mq benchBroker) *benchRunner {
	return &benchRunner{cfg: cfg, dataDir: dataDir, mq: mq}
}

func (r *benchRunner) Run(ctx context.Context) error {
	if err := r.mq.CreateTopic(ctx, r.cfg.topic, r.cfg.partitions); err != nil {
		return fmt.Errorf("create topic: %w", err)
	}

	runCtx, cancel := context.WithTimeout(ctx, r.cfg.duration)
	defer cancel()
	result := runWorkers(runCtx, r.mq, r.cfg)
	r.cfg.activeConsumers = result.activeConsumers
	printReport(r.cfg, r.dataDir.path, result.elapsed, result.counters, result.publish, result.fetch)
	return nil
}
