package broker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/logx"
)

const (
	brokerLifecycleEvents = 256
	brokerEventWorkers    = 4

	lifecycleBrokerStart    = "broker-start"
	lifecycleSchedulerStart = "scheduler-start"
	lifecycleTCPStart       = "tcp-start"
	lifecycleAdminStart     = "admin-start"
	lifecycleAdminStop      = "admin-stop"
	lifecycleTCPStop        = "tcp-stop"
	lifecycleSchedulerStop  = "scheduler-stop"
	lifecycleEventStop      = "event-recorder-stop"
	lifecycleBrokerStop     = "broker-stop"
	lifecycleLogStoreClose  = "log-store-close"
	lifecycleMetaStoreClose = "metadata-store-close"
	lifecycleLoggerClose    = "logger-close"
)

func Run(ctx context.Context) error {
	return RunWithConfigSource(ctx, NewConfigSource())
}

func RunWithConfig(ctx context.Context, cfg Config) error {
	app, err := NewApp(cfg)
	if err != nil {
		return err
	}
	return wrapBroker("app_run_failed", app.RunContext(ctx), "run broker app")
}

func RunWithConfigSource(ctx context.Context, source ConfigSource) error {
	app, err := NewAppFromConfigSource(source)
	if err != nil {
		return err
	}
	return wrapBroker("app_run_failed", app.RunContext(ctx), "run broker app")
}

func NewApp(cfg Config) (*dix.App, error) {
	normalizeConfig(&cfg)
	eventRecorder := dix.NewEventRecorder(brokerLifecycleEvents)
	return newAppWithModules(brokerAppModulesFromConfig(cfg, eventRecorder))
}

func NewAppFromConfigSource(source ConfigSource) (*dix.App, error) {
	eventRecorder := dix.NewEventRecorder(brokerLifecycleEvents)
	return newAppWithModules(brokerAppModulesFromConfigSource(source, eventRecorder))
}

func newAppWithModules(modules []dix.Module) (*dix.App, error) {
	app := dix.New("ech0",
		dix.RecentEvents(brokerLifecycleEvents),
		dix.LifecycleConcurrency(4),
		dix.Modules(modules...),
	)
	if err := app.Validate(); err != nil {
		return nil, wrapBroker("app_validation_failed", err, "validate broker app")
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

type dixEventLogger struct {
	recorder *dix.EventRecorder
	next     dix.EventLogger
}

func newDIXEventLogger(logger *slog.Logger, recorder *dix.EventRecorder) dix.EventLogger {
	return dixEventLogger{recorder: recorder, next: dix.NewSlogEventLogger(logger)}
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
