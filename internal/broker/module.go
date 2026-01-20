package broker

import "go.uber.org/fx"

var Module = fx.Module("broker",
	fx.Provide(
		NewBroker,
	),
)
