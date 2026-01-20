package logger

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/DaiYuANg/ech0/internal/config"
	slogzerolog "github.com/samber/slog-zerolog/v2"

	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"

	oopszerolog "github.com/samber/oops/loggers/zerolog"

	"go.uber.org/fx"
	"gopkg.in/natefinch/lumberjack.v2"
)

var Module = fx.Module("logger_module",
	fx.Provide(
		newZerolog,
		newSlog,
	),
)

func newZerolog(
	lc fx.Lifecycle,
	cfg *config.Config,
) zerolog.Logger {
	var writers []io.Writer
	var closers []io.Closer

	// Console
	if cfg.Logger.Console.Enabled {
		writers = append(writers, zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: "2006-01-02 15:04:05",
		})
	}

	// File
	if cfg.Logger.File.Enabled {
		lj := &lumberjack.Logger{
			Filename:   cfg.Logger.File.Path,
			MaxSize:    cfg.Logger.File.MaxSize,
			MaxAge:     cfg.Logger.File.MaxAge,
			MaxBackups: cfg.Logger.File.MaxFiles,
		}
		writers = append(writers, lj)
		closers = append(closers, lj)
	}

	if len(writers) == 0 {
		writers = append(writers, os.Stdout)
	}

	level, err := zerolog.ParseLevel(cfg.Logger.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	mw := io.MultiWriter(writers...)

	logger := zerolog.New(mw).
		Level(level).
		With().
		Timestamp().
		Logger()

	zerolog.ErrorStackMarshaler = oopszerolog.OopsStackMarshaller
	zerolog.ErrorMarshalFunc = oopszerolog.OopsMarshalFunc

	// 设置全局 logger（给不走 DI 的地方用）
	zlog.Logger = logger

	// lifecycle
	if len(closers) > 0 {
		lc.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				for _, c := range closers {
					_ = c.Close()
				}
				return nil
			},
		})
	}

	return logger
}

/*
slog facade
*/

func newSlog(zlogger zerolog.Logger) *slog.Logger {
	// 使用官方 slog-zerolog adapter
	handler := slogzerolog.Option{
		Level:     slog.LevelDebug,
		Logger:    &zlogger,
		AddSource: true,
	}.NewZerologHandler()

	logger := slog.New(handler)

	// 设置为全局 slog
	slog.SetDefault(logger)

	return logger
}
