package broker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/eventx"
	"github.com/arcgolabs/logx"
)

const (
	brokerLifecycleEvents = 256

	lifecycleBrokerStart    = "broker-start"
	lifecycleSchedulerStart = "scheduler-start"
	lifecycleTCPStart       = "tcp-start"
	lifecycleAdminStart     = "admin-start"
	lifecycleAdminStop      = "admin-stop"
	lifecycleTCPStop        = "tcp-stop"
	lifecycleSchedulerStop  = "scheduler-stop"
	lifecycleBrokerStop     = "broker-stop"
	lifecycleLogStoreClose  = "log-store-close"
	lifecycleMetaStoreClose = "metadata-store-close"
	lifecycleLoggerClose    = "logger-close"
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
	eventRecorder := dix.NewEventRecorder(brokerLifecycleEvents)
	eventLogger := newDIXEventLogger(eventRecorder, dix.NewSlogEventLogger(logger))
	module := newBrokerAppModule(cfg, logger, eventRecorder)
	app := dix.New("ech0",
		dix.UseLogger(logger),
		dix.UseEventLogger(eventLogger),
		dix.RecentEvents(brokerLifecycleEvents),
		dix.LifecycleConcurrency(4),
		dix.Modules(module),
	)
	if err := app.Validate(); err != nil {
		wrapped := wrapBroker("app_validation_failed", err, "validate broker app")
		if closeErr := logx.Close(logger); closeErr != nil {
			return nil, errors.Join(wrapped, wrapBroker("logger_close_failed", closeErr, "close logger after validation failure"))
		}
		return nil, wrapped
	}
	return app, nil
}

func newBrokerAppModule(cfg Config, logger *slog.Logger, eventRecorder *dix.EventRecorder) dix.Module {
	hooks := append(brokerStartHooks(), brokerStopServiceHooks()...)
	hooks = append(hooks, brokerStopResourceHooks()...)
	return dix.NewModule("ech0",
		brokerAppProviders(cfg, logger, eventRecorder),
		dix.Hooks(hooks...),
	)
}

func brokerAppProviders(cfg Config, logger *slog.Logger, eventRecorder *dix.EventRecorder) dix.ModuleOption {
	return dix.Providers(
		dix.Value(cfg, dix.Eager()),
		dix.Value(logger, dix.Eager()),
		dix.Value(eventRecorder, dix.Eager()),
		dix.Provider0(func() eventx.BusRuntime { return eventx.New() }, dix.Eager()),
		dix.Provider2(NewMetricsRuntime, dix.Eager()),
		dix.ProviderErr2(openAppLogStore, dix.Eager()),
		dix.Provider0(func() metadataStore { return store.NewMemoryStore() }, dix.Eager()),
		dix.ProviderErr6(newAppBroker, dix.Eager()),
		dix.ProviderErr3(NewScheduledRuntime, dix.Eager()),
		dix.Provider4(NewTCPServer, dix.Eager()),
		dix.Provider5(newAppAdminServer, dix.Eager()),
	)
}

func openAppLogStore(cfg Config, metrics *MetricsRuntime) (*store.StorxLogStore, error) {
	logStore, err := store.OpenStorxLogStoreWithOptions(cfg.SegmentLogPath(), store.StorxLogOptions{
		Metrics:  metrics,
		ReadMode: store.SegmentReadMode(cfg.Storage.SegmentReadMode),
	})
	if err != nil {
		return nil, wrapBroker("log_store_open_failed", err, "open broker log store")
	}
	return logStore, nil
}

func newAppBroker(
	cfg Config,
	logger *slog.Logger,
	bus eventx.BusRuntime,
	metrics *MetricsRuntime,
	logStore *store.StorxLogStore,
	metaStore metadataStore,
) (*Broker, error) {
	return NewWithStores(cfg, logStore, metaStore, WithLogger(logger), WithEventBus(bus), WithMetrics(metrics))
}

func newAppAdminServer(
	cfg Config,
	broker *Broker,
	logger *slog.Logger,
	metrics *MetricsRuntime,
	events *dix.EventRecorder,
) *AdminServer {
	return NewAdminServer(cfg, broker, logger, metrics, events)
}

func brokerStartHooks() []dix.HookFunc {
	return []dix.HookFunc{
		dix.OnStart(func(ctx context.Context, broker *Broker) error { return broker.Start(ctx) },
			dix.LifecycleName(lifecycleBrokerStart),
			dix.LifecyclePriority(10),
			dix.LifecycleTimeout(30*time.Second),
		),
		dix.OnStart(func(ctx context.Context, scheduled *ScheduledRuntime) error { return scheduled.Start(ctx) },
			dix.LifecycleName(lifecycleSchedulerStart),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(30*time.Second),
		),
		dix.OnStart(func(ctx context.Context, server *TCPServer) error { return server.Start(ctx) },
			dix.LifecycleName(lifecycleTCPStart),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(30*time.Second),
		),
		dix.OnStart(func(ctx context.Context, server *AdminServer) error { return server.Start(ctx) },
			dix.LifecycleName(lifecycleAdminStart),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(30*time.Second),
		),
	}
}

func brokerStopServiceHooks() []dix.HookFunc {
	return []dix.HookFunc{
		dix.OnStop(func(ctx context.Context, server *AdminServer) error { return server.Stop(ctx) },
			dix.LifecycleName(lifecycleAdminStop),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
		),
		dix.OnStop(func(ctx context.Context, server *TCPServer) error { return server.Stop(ctx) },
			dix.LifecycleName(lifecycleTCPStop),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
		),
		dix.OnStop(func(ctx context.Context, scheduled *ScheduledRuntime) error { return scheduled.Stop(ctx) },
			dix.LifecycleName(lifecycleSchedulerStop),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
		),
		dix.OnStop(func(ctx context.Context, broker *Broker) error { return broker.Stop(ctx) },
			dix.LifecycleName(lifecycleBrokerStop),
			dix.LifecyclePriority(10),
			dix.LifecycleTimeout(30*time.Second),
		),
	}
}

func brokerStopResourceHooks() []dix.HookFunc {
	return []dix.HookFunc{
		dix.OnStop(func(_ context.Context, logStore *store.StorxLogStore) error { return logStore.Close() },
			dix.LifecycleName(lifecycleLogStoreClose),
			dix.LifecyclePriority(0),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
		),
		dix.OnStop(func(_ context.Context, metaStore metadataStore) error {
			closer, ok := metaStore.(interface{ Close() error })
			if !ok {
				return nil
			}
			return closer.Close()
		},
			dix.LifecycleName(lifecycleMetaStoreClose),
			dix.LifecyclePriority(0),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
		),
		dix.OnStop(func(_ context.Context, logger *slog.Logger) error { return logx.Close(logger) },
			dix.LifecycleName(lifecycleLoggerClose),
			dix.LifecyclePriority(-10),
			dix.LifecycleTimeout(5*time.Second),
		),
	}
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

type dixEventLogger struct {
	recorder *dix.EventRecorder
	next     dix.EventLogger
}

func newDIXEventLogger(recorder *dix.EventRecorder, next dix.EventLogger) dix.EventLogger {
	return dixEventLogger{recorder: recorder, next: next}
}

func (l dixEventLogger) LogEvent(ctx context.Context, event dix.Event) {
	if l.recorder != nil {
		l.recorder.LogEvent(ctx, event)
	}
	if l.next != nil {
		l.next.LogEvent(ctx, event)
	}
}

func (l dixEventLogger) Enabled(ctx context.Context, level dix.EventLevel) bool {
	if enabler, ok := l.next.(interface {
		Enabled(context.Context, dix.EventLevel) bool
	}); ok {
		return enabler.Enabled(ctx, level)
	}
	return l.next != nil || l.recorder != nil
}
