package broker

import (
	"context"
	"log/slog"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/eventx"
	"github.com/arcgolabs/logx"
	"github.com/lyonbrown4d/ech0/store"
)

func brokerAppModulesFromConfig(cfg Config, eventRecorder *dix.EventRecorder) []dix.Module {
	return collectionlist.NewList(
		newBrokerCoreModule(brokerConfigProviders(cfg), eventRecorder),
		newBrokerSchedulerModule(),
		newBrokerTransportModule(),
		newBrokerAdminModule(),
	).Values()
}

func brokerAppModulesFromConfigSource(source ConfigSource, eventRecorder *dix.EventRecorder) []dix.Module {
	return collectionlist.NewList(
		newBrokerCoreModule(brokerConfigSourceProviders(source), eventRecorder),
		newBrokerSchedulerModule(),
		newBrokerTransportModule(),
		newBrokerAdminModule(),
	).Values()
}

func newBrokerCoreModule(configProviders []dix.ProviderFunc, eventRecorder *dix.EventRecorder) dix.Module {
	return dix.NewModule("ech0-core",
		brokerCoreProviders(configProviders, eventRecorder),
		dix.Hooks(brokerCoreHooks()...),
	)
}

func brokerConfigProviders(cfg Config) []dix.ProviderFunc {
	return []dix.ProviderFunc{dix.Value(cfg, dix.Eager())}
}

func brokerConfigSourceProviders(source ConfigSource) []dix.ProviderFunc {
	return []dix.ProviderFunc{
		dix.Value(source, dix.Eager()),
		dix.ProviderErr1(LoadConfigFromSource, dix.Eager()),
	}
}

func brokerCoreProviders(configProviders []dix.ProviderFunc, eventRecorder *dix.EventRecorder) dix.ModuleOption {
	providers := collectionlist.NewList[dix.ProviderFunc]()
	providers.Add(configProviders...)
	providers.Add(
		dix.Value(eventRecorder, dix.Eager()),
		dix.ProviderErr1(newLogger, dix.Eager()),
		dix.Provider2(newDIXEventLogger, dix.Eager()),
		dix.Provider2(NewMetricsRuntime, dix.Eager()),
		dix.Provider3(newBrokerEventBus, dix.Eager()),
		dix.ProviderErr3(newBrokerEventRecorder, dix.Eager()),
		dix.ProviderErr2(openAppLogStore, dix.Eager()),
		dix.Provider0(func() metadataStore { return store.NewMemoryStore() }, dix.Eager()),
		dix.ProviderErr6(newAppBroker, dix.Eager()),
	)
	return dix.Providers(providers.Values()...)
}

func newBrokerSchedulerModule() dix.Module {
	return dix.NewModule("ech0-scheduler",
		dix.Providers(dix.ProviderErr3(NewScheduledRuntime, dix.Eager())),
		dix.Hooks(brokerSchedulerHooks()...),
	)
}

func newBrokerTransportModule() dix.Module {
	return dix.NewModule("ech0-transport",
		dix.Providers(dix.Provider4(NewTCPServer, dix.Eager())),
		dix.Hooks(brokerTCPHooks()...),
	)
}

func newBrokerAdminModule() dix.Module {
	return dix.NewModule("ech0-admin",
		dix.Providers(dix.Provider6(newAppAdminServer, dix.Eager())),
		dix.Hooks(brokerAdminHooks()...),
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
	brokerEvents *BrokerEventRecorder,
) *AdminServer {
	server := NewAdminServer(cfg, broker, logger, metrics, events)
	server.brokerEvents = brokerEvents
	return server
}

func brokerCoreHooks() []dix.HookFunc {
	hooks := collectionlist.NewList(
		dix.OnStart(func(ctx context.Context, broker *Broker) error { return broker.Start(ctx) },
			dix.LifecycleName(lifecycleBrokerStart),
			dix.LifecyclePriority(10),
			dix.LifecycleTimeout(30*time.Second),
		),
		dix.OnStop(func(ctx context.Context, recorder *BrokerEventRecorder) error { return recorder.Close(ctx) },
			dix.LifecycleName(lifecycleEventStop),
			dix.LifecyclePriority(15),
			dix.LifecycleTimeout(5*time.Second),
		),
		dix.OnStop(func(ctx context.Context, broker *Broker) error { return broker.Stop(ctx) },
			dix.LifecycleName(lifecycleBrokerStop),
			dix.LifecyclePriority(10),
			dix.LifecycleTimeout(30*time.Second),
		),
	)
	hooks.Add(brokerStopResourceHooks()...)
	return hooks.Values()
}

func brokerSchedulerHooks() []dix.HookFunc {
	return []dix.HookFunc{
		dix.OnStart(func(ctx context.Context, scheduled *ScheduledRuntime) error { return scheduled.Start(ctx) },
			dix.LifecycleName(lifecycleSchedulerStart),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(30*time.Second),
		),
		dix.OnStop(func(ctx context.Context, scheduled *ScheduledRuntime) error { return scheduled.Stop(ctx) },
			dix.LifecycleName(lifecycleSchedulerStop),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
		),
	}
}

func brokerTCPHooks() []dix.HookFunc {
	return []dix.HookFunc{
		dix.OnStart(func(ctx context.Context, server *TCPServer) error { return server.Start(ctx) },
			dix.LifecycleName(lifecycleTCPStart),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(30*time.Second),
		),
		dix.OnStop(func(ctx context.Context, server *TCPServer) error { return server.Stop(ctx) },
			dix.LifecycleName(lifecycleTCPStop),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
		),
	}
}

func brokerAdminHooks() []dix.HookFunc {
	return []dix.HookFunc{
		dix.OnStart(func(ctx context.Context, server *AdminServer) error { return server.Start(ctx) },
			dix.LifecycleName(lifecycleAdminStart),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(30*time.Second),
		),
		dix.OnStop(func(ctx context.Context, server *AdminServer) error { return server.Stop(ctx) },
			dix.LifecycleName(lifecycleAdminStop),
			dix.LifecyclePriority(20),
			dix.LifecycleParallel(),
			dix.LifecycleTimeout(10*time.Second),
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
