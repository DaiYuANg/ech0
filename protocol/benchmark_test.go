package protocol_test

import (
	"strconv"
	"testing"

	"github.com/lyonbrown4d/ech0/protocol"
	protocolbinary "github.com/lyonbrown4d/ech0/protocol/binary"
)

var protocolBenchmarkSink []byte

func BenchmarkEncodeProduceRequest(b *testing.B) {
	partition := uint32(3)
	req := protocol.ProduceRequest{
		Topic:        "orders",
		Partition:    &partition,
		Partitioning: protocol.ProducePartitioningExplicit,
		Key:          []byte("customer-42"),
		Headers: []protocol.MessageHeader{
			{Key: "trace_id", Value: []byte("trace-1")},
			{Key: "source", Value: []byte("benchmark")},
		},
		Payload: benchmarkPayload(1024),
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(req.Payload)))
	for b.Loop() {
		body, err := protocolbinary.EncodeBody(protocol.CmdProduceRequest, req)
		if err != nil {
			b.Fatal(err)
		}
		protocolBenchmarkSink = body
	}
}

func BenchmarkDecodeProduceRequest(b *testing.B) {
	partition := uint32(3)
	body, err := protocolbinary.EncodeBody(protocol.CmdProduceRequest, protocol.ProduceRequest{
		Topic:        "orders",
		Partition:    &partition,
		Partitioning: protocol.ProducePartitioningExplicit,
		Key:          []byte("customer-42"),
		Headers: []protocol.MessageHeader{
			{Key: "trace_id", Value: []byte("trace-1")},
			{Key: "source", Value: []byte("benchmark")},
		},
		Payload: benchmarkPayload(1024),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		var req protocol.ProduceRequest
		if err := protocolbinary.DecodeBody(protocol.CmdProduceRequest, body, &req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeFetchResponse100(b *testing.B) {
	resp := protocol.FetchResponse{
		Topic:      "orders",
		Partition:  0,
		Records:    benchmarkFetchRecords(100, 1024),
		NextOffset: 100,
	}
	b.ReportAllocs()
	b.SetBytes(100 * 1024)
	for b.Loop() {
		body, err := protocolbinary.EncodeBody(protocol.CmdFetchResponse, resp)
		if err != nil {
			b.Fatal(err)
		}
		protocolBenchmarkSink = body
	}
}

func BenchmarkDecodeFetchResponse100(b *testing.B) {
	body, err := protocolbinary.EncodeBody(protocol.CmdFetchResponse, protocol.FetchResponse{
		Topic:      "orders",
		Partition:  0,
		Records:    benchmarkFetchRecords(100, 1024),
		NextOffset: 100,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(body)))
	for b.Loop() {
		var resp protocol.FetchResponse
		if err := protocolbinary.DecodeBody(protocol.CmdFetchResponse, body, &resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeStartRequest(b *testing.B) {
	timeoutMS := uint64(5000)
	pollIntervalMS := uint64(5)
	partition := uint32(0)
	req := protocol.StartRequestRequest{
		Subject:        "svc.echo",
		InstanceID:     "A1",
		TimeoutMS:      &timeoutMS,
		PollIntervalMS: &pollIntervalMS,
		Partition:      &partition,
		Partitioning:   protocol.ProducePartitioningExplicit,
		Headers:        []protocol.MessageHeader{{Key: "trace_id", Value: []byte("trace-1")}},
		Payload:        benchmarkPayload(512),
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(req.Payload)))
	for b.Loop() {
		body, err := protocolbinary.EncodeBody(protocol.CmdStartRequestRequest, req)
		if err != nil {
			b.Fatal(err)
		}
		protocolBenchmarkSink = body
	}
}

func benchmarkFetchRecords(count, payloadBytes int) []protocol.FetchRecord {
	records := make([]protocol.FetchRecord, 0, count)
	payload := benchmarkPayload(payloadBytes)
	for i := range count {
		records = append(records, protocol.FetchRecord{
			Offset:      uint64(i),
			TimestampMS: 1700000000000 + uint64(i),
			Key:         []byte("customer-" + strconv.Itoa(i%128)),
			Headers: []protocol.MessageHeader{
				{Key: "trace_id", Value: []byte("trace-" + strconv.Itoa(i))},
				{Key: "source", Value: []byte("benchmark")},
			},
			Payload: payload,
		})
	}
	return records
}

func benchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}
