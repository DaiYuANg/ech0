package http

import (
	"github.com/gofiber/fiber/v2"
	"go.uber.org/fx"
)

var Module = fx.Module("http",
	fx.Provide(
		newFiber,
	),
	fx.Invoke(
		lifecycle,
	),
)

func newFiber() *fiber.App {
	return fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
}

func lifecycle(lc fx.Lifecycle, app *fiber.App) {
	lc.Append(
		fx.StartHook(func() {
			go app.Listen("0.0.0.0:9989")
		}),
	)
}
