package broker

import (
	"context"
	"log/slog"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/eventx"
	"github.com/arcgolabs/observabilityx"
)

type TopicCreatedEvent struct {
	Topic      string
	Partitions uint32
}

func (TopicCreatedEvent) Name() string { return "ech0.topic.created" }

type RecordProducedEvent struct {
	Topic      string
	Partition  uint32
	Offset     uint64
	NextOffset uint64
}

func (RecordProducedEvent) Name() string { return "ech0.record.produced" }

type DirectMessageSentEvent struct {
	Sender    string
	Recipient string
	Offset    uint64
}

func (DirectMessageSentEvent) Name() string { return "ech0.direct.sent" }

type BrokerEventSummary struct {
	At     time.Time         `json:"at"`
	Name   string            `json:"name"`
	Fields map[string]string `json:"fields,omitempty"`
}

type BrokerEventRecorder struct {
	events       *collectionlist.ConcurrentRingBuffer[BrokerEventSummary]
	unsubscribes *collectionlist.List[func()]
}

func newBrokerEventBus(cfg Config, logger *slog.Logger, metrics *MetricsRuntime) eventx.BusRuntime {
	obs := observabilityx.NopWithLogger(logger)
	if metrics != nil {
		obs = metrics.obs
	}
	return eventx.New(
		eventx.WithAntsPool(brokerEventWorkerCount(cfg)),
		eventx.WithParallelDispatch(true),
		eventx.WithObservability(obs),
		eventx.WithMiddleware(
			eventx.RecoverMiddleware(),
			eventx.ObserveMiddleware(newBrokerEventObserver(logger)),
		),
		eventx.WithAsyncErrorHandler(newBrokerAsyncEventErrorHandler(logger)),
	)
}

func brokerEventWorkerCount(cfg Config) int {
	if cfg.Broker.MaintenanceConcurrency <= int64(brokerEventWorkers) {
		return brokerEventWorkers
	}
	if cfg.Broker.MaintenanceConcurrency > 64 {
		return 64
	}
	return int(cfg.Broker.MaintenanceConcurrency)
}

func newBrokerEventObserver(logger *slog.Logger) func(context.Context, eventx.Event, time.Duration, error) {
	return func(_ context.Context, event eventx.Event, duration time.Duration, err error) {
		if err == nil || logger == nil {
			return
		}
		logger.Warn("broker event handler failed",
			"event", event.Name(),
			"duration", duration,
			"error", err,
		)
	}
}

func newBrokerAsyncEventErrorHandler(logger *slog.Logger) func(context.Context, eventx.Event, error) {
	return func(_ context.Context, event eventx.Event, err error) {
		if logger == nil {
			return
		}
		logger.Warn("broker async event dispatch failed",
			"event", event.Name(),
			"error", err,
		)
	}
}

func newBrokerEventRecorder(cfg Config, logger *slog.Logger, bus eventx.BusRuntime) (*BrokerEventRecorder, error) {
	recorder := &BrokerEventRecorder{
		events:       collectionlist.NewConcurrentRingBuffer[BrokerEventSummary](brokerLifecycleEvents),
		unsubscribes: collectionlist.NewList[func()](),
	}
	if !cfg.Admin.DebugEnabled {
		return recorder, nil
	}
	if err := recorder.subscribe(bus); err != nil {
		if closeErr := recorder.Close(context.Background()); closeErr != nil && logger != nil {
			logger.Warn("broker event recorder cleanup failed", "error", closeErr)
		}
		if logger != nil {
			logger.Warn("broker event recorder setup failed", "error", err)
		}
		return nil, err
	}
	return recorder, nil
}

func (r *BrokerEventRecorder) subscribe(bus eventx.BusRuntime) error {
	if err := subscribeBrokerEvent[TopicCreatedEvent](bus, r); err != nil {
		return wrapBroker("event_recorder_subscribe_failed", err, "subscribe topic-created broker events")
	}
	if err := subscribeBrokerEvent[RecordProducedEvent](bus, r); err != nil {
		return wrapBroker("event_recorder_subscribe_failed", err, "subscribe record-produced broker events")
	}
	if err := subscribeBrokerEvent[DirectMessageSentEvent](bus, r); err != nil {
		return wrapBroker("event_recorder_subscribe_failed", err, "subscribe direct-message broker events")
	}
	return nil
}

func subscribeBrokerEvent[T eventx.Event](bus eventx.BusRuntime, recorder *BrokerEventRecorder) error {
	unsubscribe, err := eventx.Subscribe[T](bus, func(ctx context.Context, event T) error {
		recorder.Record(ctx, event)
		return nil
	})
	if err != nil {
		return err
	}
	recorder.unsubscribes.Add(unsubscribe)
	return nil
}

func (r *BrokerEventRecorder) Record(ctx context.Context, event eventx.Event) {
	_ = ctx
	if r == nil || r.events == nil || event == nil {
		return
	}
	r.events.Push(BrokerEventSummary{
		At:     time.Now(),
		Name:   event.Name(),
		Fields: eventFields(event),
	})
}

func (r *BrokerEventRecorder) Events() []BrokerEventSummary {
	if r == nil || r.events == nil {
		return []BrokerEventSummary{}
	}
	values := r.events.Values()
	if values == nil {
		return []BrokerEventSummary{}
	}
	return values
}

func (r *BrokerEventRecorder) Close(ctx context.Context) error {
	_ = ctx
	if r == nil || r.unsubscribes == nil {
		return nil
	}
	r.unsubscribes.Range(func(_ int, unsubscribe func()) bool {
		if unsubscribe != nil {
			unsubscribe()
		}
		return true
	})
	r.unsubscribes.Clear()
	return nil
}
