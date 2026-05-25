package transport_test

import (
	"bytes"
	"testing"

	"github.com/lyonbrown4d/ech0/transport"
)

var transportBenchmarkSink transport.Frame

func BenchmarkWriteFrame1KB(b *testing.B) {
	frame := mustBenchmarkFrame(b, 1024)
	var out bytes.Buffer
	b.ReportAllocs()
	b.SetBytes(int64(len(frame.Body)))
	for b.Loop() {
		out.Reset()
		if err := transport.WriteFrame(&out, frame); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadFrame1KB(b *testing.B) {
	frame := mustBenchmarkFrame(b, 1024)
	var encoded bytes.Buffer
	if err := transport.WriteFrame(&encoded, frame); err != nil {
		b.Fatal(err)
	}
	raw := encoded.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(frame.Body)))
	for b.Loop() {
		read, err := transport.ReadFrame(bytes.NewReader(raw))
		if err != nil {
			b.Fatal(err)
		}
		transportBenchmarkSink = read
	}
}

func BenchmarkReadFrame1KBPooled(b *testing.B) {
	frame := mustBenchmarkFrame(b, 1024)
	var encoded bytes.Buffer
	if err := transport.WriteFrame(&encoded, frame); err != nil {
		b.Fatal(err)
	}
	raw := encoded.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(frame.Body)))
	for b.Loop() {
		read, err := transport.ReadFrameWithLimitPooled(bytes.NewReader(raw), 0)
		if err != nil {
			b.Fatal(err)
		}
		transportBenchmarkSink = read.Frame
		read.Release()
	}
}

func BenchmarkReadFrame64KB(b *testing.B) {
	frame := mustBenchmarkFrame(b, 64*1024)
	var encoded bytes.Buffer
	if err := transport.WriteFrame(&encoded, frame); err != nil {
		b.Fatal(err)
	}
	raw := encoded.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(frame.Body)))
	for b.Loop() {
		read, err := transport.ReadFrame(bytes.NewReader(raw))
		if err != nil {
			b.Fatal(err)
		}
		transportBenchmarkSink = read
	}
}

func BenchmarkReadFrame64KBPooled(b *testing.B) {
	frame := mustBenchmarkFrame(b, 64*1024)
	var encoded bytes.Buffer
	if err := transport.WriteFrame(&encoded, frame); err != nil {
		b.Fatal(err)
	}
	raw := encoded.Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(frame.Body)))
	for b.Loop() {
		read, err := transport.ReadFrameWithLimitPooled(bytes.NewReader(raw), 0)
		if err != nil {
			b.Fatal(err)
		}
		transportBenchmarkSink = read.Frame
		read.Release()
	}
}

func mustBenchmarkFrame(b *testing.B, bodyBytes int) transport.Frame {
	b.Helper()
	body := make([]byte, bodyBytes)
	for i := range body {
		body[i] = byte(i)
	}
	frame, err := transport.NewFrame(2, 20, body)
	if err != nil {
		b.Fatal(err)
	}
	return frame
}
