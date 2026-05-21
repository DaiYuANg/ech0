package broker

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const brokerTracerName = "github.com/lyonbrown4d/ech0/broker"

type brokerTracer = trace.Tracer

func newNoopBrokerTracer() brokerTracer {
	return noop.NewTracerProvider().Tracer(brokerTracerName)
}

func (b *Broker) brokerTracer() brokerTracer {
	if b == nil || b.tracer == nil {
		return newNoopBrokerTracer()
	}
	return b.tracer
}

func (b *Broker) topicSpanOptions(ctx context.Context, topic string, partition *uint32) []trace.SpanStartOption {
	identity := b.identity(ctx)
	attrs := []attribute.KeyValue{
		attribute.String("messaging.system", "ech0"),
		attribute.String("messaging.destination.name", topic),
		attribute.String("ech0.tenant", identity.Tenant),
		attribute.String("ech0.namespace", identity.Namespace),
		attribute.String("ech0.principal", identity.Principal),
	}
	if partition != nil {
		attrs = append(attrs, attribute.Int64("messaging.destination.partition.id", int64(*partition)))
	}
	return []trace.SpanStartOption{trace.WithAttributes(attrs...)}
}

func (b *Broker) consumerSpanOptions(
	ctx context.Context,
	consumer string,
	topic string,
	partition uint32,
) []trace.SpanStartOption {
	identity := b.identity(ctx)
	return []trace.SpanStartOption{
		trace.WithAttributes(
			attribute.String("messaging.system", "ech0"),
			attribute.String("messaging.destination.name", topic),
			attribute.Int64("messaging.destination.partition.id", int64(partition)),
			attribute.String("messaging.consumer.id", consumer),
			attribute.String("ech0.tenant", identity.Tenant),
			attribute.String("ech0.namespace", identity.Namespace),
			attribute.String("ech0.principal", identity.Principal),
		),
	}
}

func recordBrokerSpanError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
