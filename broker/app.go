package broker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/DaiYuANg/ech0/store"
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
	return app.RunContext(ctx)
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
			dix.ProviderErr1(func(cfg Config) (*store.StorxLogStore, error) {
				return store.OpenStorxLogStore(cfg.SegmentLogPath())
			}),
			dix.ProviderErr1(func(cfg Config) (*store.StorxMetadataStore, error) {
				return store.OpenStorxMetadataStore(cfg.MetadataPath())
			}),
			dix.ProviderErr5(func(cfg Config, logger *slog.Logger, bus eventx.BusRuntime, logStore *store.StorxLogStore, metaStore *store.StorxMetadataStore) (*Broker, error) {
				return NewWithStores(cfg, logStore, metaStore, WithLogger(logger), WithEventBus(bus))
			}),
			dix.ProviderErr3(func(cfg Config, broker *Broker, logger *slog.Logger) (*ScheduledRuntime, error) {
				return NewScheduledRuntime(cfg, broker, logger)
			}),
			dix.Provider3(func(cfg Config, broker *Broker, logger *slog.Logger) *TCPServer {
				return NewTCPServer(cfg, broker, logger)
			}),
			dix.Provider3(func(cfg Config, broker *Broker, logger *slog.Logger) *AdminServer {
				return NewAdminServer(cfg, broker, logger)
			}),
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
		_ = logx.Close(logger)
		return nil, err
	}
	return app, nil
}

func newLogger(cfg Config) (*slog.Logger, error) {
	opts := []logx.Option{
		logx.WithConsole(cfg.Logging.EnableStdout),
		logx.WithLevelString(cfg.Logging.Level),
		logx.WithCaller(true),
	}
	if !cfg.Logging.ANSI {
		opts = append(opts, logx.WithNoColor())
	}
	if cfg.Logging.EnableFile {
		path := cfg.LogFilePath()
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, err
		}
		opts = append(opts, logx.WithFile(path))
	}
	return logx.New(opts...)
}
