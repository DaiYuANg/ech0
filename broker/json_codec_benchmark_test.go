package broker_test

import (
	stdjson "encoding/json"
	"testing"

	"github.com/bytedance/sonic"
	goccyjson "github.com/goccy/go-json"
	"github.com/lyonbrown4d/ech0/store"
)

var jsonBenchmarkBytesSink []byte
var jsonBenchmarkEnvelopeSink jsonBenchmarkEnvelope

type jsonBenchmarkEnvelope struct {
	Type          string               `json:"type"`
	Subject       string               `json:"subject"`
	CorrelationID string               `json:"correlation_id"`
	ReplyTo       string               `json:"reply_to,omitempty"`
	SenderID      string               `json:"sender_id,omitempty"`
	ExpiresAtMS   uint64               `json:"expires_at_ms,omitempty"`
	Headers       []store.RecordHeader `json:"headers,omitempty"`
	Error         *string              `json:"error,omitempty"`
	Payload       []byte               `json:"payload,omitempty"`
}

func BenchmarkJSONMarshalStdRequestReplyEnvelope(b *testing.B) {
	envelope := benchmarkRequestReplyEnvelope()
	b.ReportAllocs()
	for b.Loop() {
		payload, err := stdjson.Marshal(envelope)
		if err != nil {
			b.Fatal(err)
		}
		jsonBenchmarkBytesSink = payload
	}
}

func BenchmarkJSONMarshalGoccyRequestReplyEnvelope(b *testing.B) {
	envelope := benchmarkRequestReplyEnvelope()
	b.ReportAllocs()
	for b.Loop() {
		payload, err := goccyjson.Marshal(envelope)
		if err != nil {
			b.Fatal(err)
		}
		jsonBenchmarkBytesSink = payload
	}
}

func BenchmarkJSONMarshalSonicRequestReplyEnvelope(b *testing.B) {
	envelope := benchmarkRequestReplyEnvelope()
	b.ReportAllocs()
	for b.Loop() {
		payload, err := sonic.Marshal(envelope)
		if err != nil {
			b.Fatal(err)
		}
		jsonBenchmarkBytesSink = payload
	}
}

func BenchmarkJSONUnmarshalStdRequestReplyEnvelope(b *testing.B) {
	payload := mustBenchmarkJSONPayload(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		var envelope jsonBenchmarkEnvelope
		if err := stdjson.Unmarshal(payload, &envelope); err != nil {
			b.Fatal(err)
		}
		jsonBenchmarkEnvelopeSink = envelope
	}
}

func BenchmarkJSONUnmarshalGoccyRequestReplyEnvelope(b *testing.B) {
	payload := mustBenchmarkJSONPayload(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		var envelope jsonBenchmarkEnvelope
		if err := goccyjson.Unmarshal(payload, &envelope); err != nil {
			b.Fatal(err)
		}
		jsonBenchmarkEnvelopeSink = envelope
	}
}

func BenchmarkJSONUnmarshalSonicRequestReplyEnvelope(b *testing.B) {
	payload := mustBenchmarkJSONPayload(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		var envelope jsonBenchmarkEnvelope
		if err := sonic.Unmarshal(payload, &envelope); err != nil {
			b.Fatal(err)
		}
		jsonBenchmarkEnvelopeSink = envelope
	}
}

func benchmarkRequestReplyEnvelope() jsonBenchmarkEnvelope {
	return jsonBenchmarkEnvelope{
		Type:          "ech0.request.v1",
		Subject:       "svc.orders.created",
		CorrelationID: "0f6e88d77d6d45838f760f85787c09fd",
		ReplyTo:       "__reply/order-api-1/0f6e88d77d6d45838f760f85787c09fd",
		SenderID:      "order-api-1",
		ExpiresAtMS:   1700000005000,
		Headers: []store.RecordHeader{
			{Key: "trace_id", Value: []byte("trace-0000000000000001")},
			{Key: "content_type", Value: []byte("application/json")},
			{Key: "tenant", Value: []byte("acme")},
		},
		Payload: benchmarkJSONPayloadBytes(),
	}
}

func benchmarkJSONPayloadBytes() []byte {
	payload := make([]byte, 1024)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}

func mustBenchmarkJSONPayload(b *testing.B) []byte {
	b.Helper()
	payload, err := goccyjson.Marshal(benchmarkRequestReplyEnvelope())
	if err != nil {
		b.Fatal(err)
	}
	return payload
}
