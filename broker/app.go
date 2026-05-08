package broker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/eventx"
	"github.com/arcgolabs/logx"
)

func Run(ctx context.Context) error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	return RunWithConfig(ctx, cfg)
}

func RunWithConfig(ctx context.Context, cfg Config) error {
	app, err := NewApp(cfg)
	if err != nil {
		return err
	}
	return wrapBroker("app_run_failed", app.RunContext(ctx), "run broker app")
}

func NewApp(cfg Config) (*dix.App, error) {
	normalizeConfig(&cfg)
	logger, err := newLogger(cfg)
	if err != nil {
		return nil, err
	}
	module := dix.NewModule("ech0",
		dix.Providers(
			dix.Value(cfg),
			dix.Value(logger),
			dix.Provider0(func() eventx.BusRuntime { return eventx.New() }),
			dix.Provider2(NewMetricsRuntime),
			dix.ProviderErr3(func(cfg Config, logger *slog.Logger, metrics *MetricsRuntime) (*store.StorxLogStore, error) {
				return store.OpenStorxLogStoreWithOptions(cfg.SegmentLogPath(), store.StorxLogOptions{
					Logger:    logger,
					Observers: []store.StorxObserver{newStorageMetricsObserver(metrics)},
					Metrics:   metrics,
				})
			}),
			dix.ProviderErr3(func(cfg Config, logger *slog.Logger, metrics *MetricsRuntime) (*store.StorxMetadataStore, error) {
				return store.OpenStorxMetadataStoreWithOptions(cfg.MetadataPath(), store.StorxMetadataOptions{
					Logger:    logger,
					Observers: []store.StorxObserver{newStorageMetricsObserver(metrics)},
				})
			}),
			dix.ProviderErr6(func(cfg Config, logger *slog.Logger, bus eventx.BusRuntime, metrics *MetricsRuntime, logStore *store.StorxLogStore, metaStore *store.StorxMetadataStore) (*Broker, error) {
				return NewWithStores(cfg, logStore, metaStore, WithLogger(logger), WithEventBus(bus), WithMetrics(metrics))
			}),
			dix.ProviderErr3(NewScheduledRuntime),
			dix.Provider4(NewTCPServer),
			dix.Provider4(NewAdminServer),
		),
		dix.Hooks(
			dix.OnStart(func(ctx context.Context, broker *Broker) error {
				return broker.Start(ctx)
			}),
			dix.OnStart(func(ctx context.Context, scheduled *ScheduledRuntime) error {
				return scheduled.Start(ctx)
			}),
			dix.OnStart(func(ctx context.Context, server *TCPServer) error {
				return server.Start(ctx)
			}),
			dix.OnStart(func(ctx context.Context, server *AdminServer) error {
				return server.Start(ctx)
			}),
			dix.OnStop(func(_ context.Context, logger *slog.Logger) error {
				return logx.Close(logger)
			}),
			dix.OnStop(func(_ context.Context, logStore *store.StorxLogStore) error {
				return logStore.Close()
			}),
			dix.OnStop(func(_ context.Context, metaStore *store.StorxMetadataStore) error {
				return metaStore.Close()
			}),
			dix.OnStop(func(ctx context.Context, broker *Broker) error {
				return broker.Stop(ctx)
			}),
			dix.OnStop(func(ctx context.Context, scheduled *ScheduledRuntime) error {
				return scheduled.Stop(ctx)
			}),
			dix.OnStop(func(ctx context.Context, server *TCPServer) error {
				return server.Stop(ctx)
			}),
			dix.OnStop(func(ctx context.Context, server *AdminServer) error {
				return server.Stop(ctx)
			}),
		),
	)
	app := dix.New("ech0", dix.UseLogger(logger), dix.Modules(module))
	if err := app.Validate(); err != nil {
		wrapped := wrapBroker("app_validation_failed", err, "validate broker app")
		if closeErr := logx.Close(logger); closeErr != nil {
			return nil, errors.Join(wrapped, wrapBroker("logger_close_failed", closeErr, "close logger after validation failure"))
		}
		return nil, wrapped
	}
	return app, nil
}

func newLogger(cfg Config) (*slog.Logger, error) {
	opts := collectionlist.NewList(
		logx.WithConsole(cfg.Logging.EnableStdout),
		logx.WithLevelString(cfg.Logging.Level),
		logx.WithCaller(true),
	)
	if !cfg.Logging.ANSI {
		opts.Add(logx.WithNoColor())
	}
	if cfg.Logging.EnableFile {
		path := cfg.LogFilePath()
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, wrapBroker("log_directory_create_failed", err, "create log directory")
		}
		opts.Add(logx.WithFile(path))
	}
	logger, err := logx.New(opts.Values()...)
	if err != nil {
		return nil, wrapBroker("logger_create_failed", err, "create logger")
	}
	return logger, nil
}
