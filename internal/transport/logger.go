package transport

import (
	"fmt"
	"log/slog"
	"os"
)

type gnetSlogLogger struct {
	logger *slog.Logger
}

func (g *gnetSlogLogger) Infof(format string, args ...any) {
	g.logger.Info(fmt.Sprintf(format, args...))
}

func (g *gnetSlogLogger) Debugf(format string, args ...any) {
	g.logger.Debug(fmt.Sprintf(format, args...))
}

func (g *gnetSlogLogger) Warnf(format string, args ...any) {
	g.logger.Warn(fmt.Sprintf(format, args...))
}

func (g *gnetSlogLogger) Errorf(format string, args ...any) {
	g.logger.Error(fmt.Sprintf(format, args...))
}

func (g *gnetSlogLogger) Fatalf(format string, args ...any) {
	g.logger.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
