package broker

import (
	"log/slog"

	"github.com/arcgolabs/authx"
	"github.com/arcgolabs/eventx"
	"github.com/lyonbrown4d/ech0/discovery"
)

func WithLogger(logger *slog.Logger) Option {
	return func(b *Broker) {
		if logger != nil {
			b.logger = logger
		}
	}
}

func WithEventBus(events eventx.BusRuntime) Option {
	return func(b *Broker) {
		if events != nil {
			b.events = events
		}
	}
}

func WithDiscoveryProvider(provider discovery.Provider) Option {
	return func(b *Broker) {
		if provider != nil {
			b.discovery = provider
		}
	}
}

func WithMetrics(metrics *MetricsRuntime) Option {
	return func(b *Broker) {
		if metrics != nil {
			b.metrics = metrics
		}
	}
}

func WithAuthEngine(engine *authx.Engine) Option {
	return func(b *Broker) {
		if engine != nil {
			b.auth = engine
			b.aclAuthorizerConfigured = true
		}
	}
}

func WithAuthProvider(provider authx.AuthenticationProvider) Option {
	return func(b *Broker) {
		if provider == nil {
			return
		}
		if b.auth == nil {
			b.auth = newDefaultAuthEngine(b.logger)
		}
		if err := b.auth.RegisterProvider(provider); err != nil {
			if b.logger != nil {
				b.logger.Warn("register auth provider failed", "error", err)
			}
		}
	}
}

func WithACLAuthorizer(authorizer authx.Authorizer) Option {
	return func(b *Broker) {
		if authorizer != nil {
			if b.auth == nil {
				b.auth = newDefaultAuthEngine(b.logger)
			}
			b.auth.SetAuthorizer(authorizer)
			b.aclAuthorizerConfigured = true
		}
	}
}

func WithQuotaLimiter(limiter QuotaLimiter) Option {
	return func(b *Broker) {
		if limiter != nil {
			b.quota = limiter
		}
	}
}
