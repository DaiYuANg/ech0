package broker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	dragonlogger "github.com/lni/dragonboat/v4/logger"
)

var (
	dragonboatLoggerOnce sync.Once
	dragonboatSlogLogger atomic.Value
)

func configureDragonboatLogger(base *slog.Logger) {
	if base == nil {
		base = slog.Default()
	}
	dragonboatSlogLogger.Store(base.With("component", "dragonboat"))
	dragonboatLoggerOnce.Do(func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				return
			}
		}()
		dragonlogger.SetLoggerFactory(func(pkgName string) dragonlogger.ILogger {
			return newDragonboatSlogAdapter(pkgName)
		})
	})
}

type dragonboatSlogAdapter struct {
	pkgName string
	level   atomic.Int64
}

func newDragonboatSlogAdapter(pkgName string) *dragonboatSlogAdapter {
	adapter := &dragonboatSlogAdapter{pkgName: pkgName}
	adapter.SetLevel(dragonlogger.INFO)
	return adapter
}

func (l *dragonboatSlogAdapter) SetLevel(level dragonlogger.LogLevel) {
	l.level.Store(int64(level))
}

func (l *dragonboatSlogAdapter) Debugf(format string, args ...any) {
	l.logf(slog.LevelDebug, dragonlogger.DEBUG, format, args...)
}

func (l *dragonboatSlogAdapter) Infof(format string, args ...any) {
	l.logf(slog.LevelInfo, dragonlogger.INFO, format, args...)
}

func (l *dragonboatSlogAdapter) Warningf(format string, args ...any) {
	l.logf(slog.LevelWarn, dragonlogger.WARNING, format, args...)
}

func (l *dragonboatSlogAdapter) Errorf(format string, args ...any) {
	l.logf(slog.LevelError, dragonlogger.ERROR, format, args...)
}

func (l *dragonboatSlogAdapter) Panicf(format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	l.logger().Error(message, "subsystem", l.pkgName)
	panic(message)
}

func (l *dragonboatSlogAdapter) logf(slogLevel slog.Level, dragonLevel dragonlogger.LogLevel, format string, args ...any) {
	if !l.enabled(dragonLevel) {
		return
	}
	message := fmt.Sprintf(format, args...)
	logger := l.logger()
	if !logger.Enabled(context.Background(), slogLevel) {
		return
	}
	logger.Log(context.Background(), slogLevel, message, "subsystem", l.pkgName)
}

func (l *dragonboatSlogAdapter) enabled(level dragonlogger.LogLevel) bool {
	return int64(level) <= l.level.Load()
}

func (l *dragonboatSlogAdapter) logger() *slog.Logger {
	value := dragonboatSlogLogger.Load()
	if value == nil {
		return slog.Default().With("component", "dragonboat")
	}
	logger, ok := value.(*slog.Logger)
	if !ok || logger == nil {
		return slog.Default().With("component", "dragonboat")
	}
	return logger
}
