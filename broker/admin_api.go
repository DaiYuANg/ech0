package broker

import (
	"context"
	"fmt"
	"reflect"
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

type runtimeEventsOutput struct {
	Body struct {
		Events []runtimeEventSummary `json:"events"`
	} `json:"body"`
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
	_ = ctx
	topics, err := s.broker.TopicSummaries()
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

func (s *AdminServer) apiRuntimeEvents(ctx context.Context, _ *struct{}) (*runtimeEventsOutput, error) {
	_ = ctx
	out := &runtimeEventsOutput{}
	out.Body.Events = runtimeEventSummaries(s.events)
	return out, nil
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
				Kind:   runtimeEventKind(record.Event),
				Fields: runtimeEventFields(record.Event),
			})
		}
	})
	return events
}

func runtimeEventKind(event dix.Event) string {
	if event == nil {
		return ""
	}
	typ := reflect.TypeOf(event)
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.Name() != "" {
		return typ.Name()
	}
	return fmt.Sprintf("%T", event)
}

func runtimeEventFields(event dix.Event) map[string]string {
	value, ok := runtimeEventStructValue(event)
	if !ok {
		return nil
	}
	return runtimeEventStructFields(value)
}

func runtimeEventStructValue(event dix.Event) (reflect.Value, bool) {
	if event == nil {
		return reflect.Value{}, false
	}
	value := reflect.ValueOf(event)
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return reflect.Value{}, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}
	return value, true
}

func runtimeEventStructFields(value reflect.Value) map[string]string {
	typ := value.Type()
	fields := make(map[string]string, value.NumField())
	for i := range value.NumField() {
		field := typ.Field(i)
		if field.PkgPath != "" {
			continue
		}
		text, ok := runtimeEventFieldString(value.Field(i))
		if ok {
			fields[field.Name] = text
		}
	}
	if len(fields) == 0 {
		return nil
	}
	return fields
}

func runtimeEventFieldString(value reflect.Value) (string, bool) {
	if !value.IsValid() || !value.CanInterface() {
		return "", false
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "", false
		}
		value = value.Elem()
	}
	return fmt.Sprint(value.Interface()), true
}
