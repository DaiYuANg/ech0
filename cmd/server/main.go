package main

import (
	"log/slog"

	"github.com/DaiYuANg/ech0/internal/broker"
	"github.com/DaiYuANg/ech0/internal/config"
	"github.com/DaiYuANg/ech0/internal/distributed"
	"github.com/DaiYuANg/ech0/internal/http"
	"github.com/DaiYuANg/ech0/internal/logger"
	"github.com/DaiYuANg/ech0/internal/transport"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func main() {
	fx.New(
		config.Module,
		logger.Module,
		broker.Module,
		http.Module,
		transport.Module,
		distributed.Module,
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: log}
		}),
	).Run()
}
