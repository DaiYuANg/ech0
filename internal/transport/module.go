package transport

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/DaiYuANg/ech0/internal/broker"
	"github.com/panjf2000/gnet/v2"
	"github.com/samber/oops"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"gnet",
	fx.Provide(func(logger *slog.Logger, b *broker.Broker) gnet.EventHandler {
		return newEchoServer(fmt.Sprintf("tcp://:%d", 8080), logger, b)
	}),
	fx.Invoke(func(lc fx.Lifecycle, server gnet.EventHandler, logger *slog.Logger) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				go func() {
					if err := gnet.Run(
						server,
						fmt.Sprintf("tcp://:%d", 8080),
						gnet.WithMulticore(true),
						gnet.WithLogger(&gnetSlogLogger{logger}),
					); err != nil {
						logger.Error("gnet start failed",
							slog.String("addr", "127.0.0.1"),
							slog.Any("err", oops.Wrap(err)),
						)
					}
				}()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				if s, ok := server.(interface{ Stop() error }); ok {
					return s.Stop()
				}
				return nil
			},
		})
	}),
)
