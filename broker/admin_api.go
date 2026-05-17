package broker

import (
	"context"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/samber/oops"
)

type healthOutput struct {
	Body RuntimeHealth `json:"body"`
}

type topicsOutput struct {
	Body struct {
		Topics []TopicSummary `json:"topics"`
	} `json:"body"`
}

type metricsOutput struct {
	Body struct {
		Snapshot MetricsSnapshot `json:"snapshot"`
		Runtime  string          `json:"runtime"`
	} `json:"body"`
}

type quotaOutput struct {
	Body struct {
		Quota QuotaSummary `json:"quota"`
	} `json:"body"`
}

type runtimeEventsOutput struct {
	Body struct {
		Events       []runtimeEventSummary `json:"events"`
		BrokerEvents []BrokerEventSummary  `json:"broker_events,omitempty"`
	} `json:"body"`
}

type runtimeEventsStreamEvent struct {
	RuntimeEvents []runtimeEventSummary `json:"runtime_events"`
	BrokerEvents  []BrokerEventSummary  `json:"broker_events,omitempty"`
}

type runtimeEventSummary struct {
	At     time.Time         `json:"at"`
	Kind   string            `json:"kind"`
	Fields map[string]string `json:"fields,omitempty"`
}

func (s *AdminServer) apiHealth(ctx context.Context, _ *struct{}) (*healthOutput, error) {
	_ = ctx
	out := &healthOutput{}
	out.Body = s.broker.RuntimeHealth()
	return out, nil
}

func (s *AdminServer) apiTopics(ctx context.Context, _ *struct{}) (*topicsOutput, error) {
	topics, err := s.broker.TopicSummariesFor(ctx)
	if err != nil {
		return nil, wrapBroker("list_topics_failed", err, "list topics")
	}
	out := &topicsOutput{}
	out.Body.Topics = topics
	return out, nil
}

func (s *AdminServer) apiMetrics(ctx context.Context, _ *struct{}) (*metricsOutput, error) {
	if err := s.metrics.RefreshStream(ctx, s.broker); err != nil {
		return nil, oops.In("admin").Code("list_topics_failed").Wrapf(err, "build metrics")
	}
	out := &metricsOutput{}
	out.Body.Snapshot = s.metrics.Snapshot()
	out.Body.Runtime = s.broker.RuntimeHealth().RuntimeMode
	return out, nil
}

func (s *AdminServer) apiQuota(ctx context.Context, _ *struct{}) (*quotaOutput, error) {
	quota, err := s.broker.QuotaSummaryFor(ctx)
	if err != nil {
		return nil, oops.In("admin").Code("quota_summary_failed").Wrapf(err, "build quota summary")
	}
	out := &quotaOutput{}
	out.Body.Quota = quota
	return out, nil
}

func (s *AdminServer) apiRuntimeEvents(ctx context.Context, _ *struct{}) (*runtimeEventsOutput, error) {
	_ = ctx
	out := &runtimeEventsOutput{}
	out.Body.Events = runtimeEventSummaries(s.events)
	out.Body.BrokerEvents = brokerEventSummaries(s.brokerEvents)
	return out, nil
}

func (s *AdminServer) runtimeEventStreamSnapshot() runtimeEventsStreamEvent {
	return runtimeEventsStreamEvent{
		RuntimeEvents: runtimeEventSummaries(s.events),
		BrokerEvents:  brokerEventSummaries(s.brokerEvents),
	}
}

func runtimeEventSummaries(recorder *dix.EventRecorder) []runtimeEventSummary {
	if recorder == nil {
		return []runtimeEventSummary{}
	}
	records := recorder.Events()
	events := make([]runtimeEventSummary, 0, records.Len())
	records.ViewValues(func(items []dix.EventRecord) {
		for _, record := range items {
			events = append(events, runtimeEventSummary{
				At:     record.At,
				Kind:   eventKind(record.Event),
				Fields: eventFields(record.Event),
			})
		}
	})
	return events
}

func brokerEventSummaries(recorder *BrokerEventRecorder) []BrokerEventSummary {
	if recorder == nil {
		return []BrokerEventSummary{}
	}
	return recorder.Events()
}
