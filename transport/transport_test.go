package transport_test

import (
	"bytes"
	"testing"

	"github.com/lyonbrown4d/ech0/transport"
)

func TestFrameRoundTrip(t *testing.T) {
	frame, err := transport.NewFrame(1, 42, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if writeErr := transport.WriteFrame(&buf, frame); writeErr != nil {
		t.Fatal(writeErr)
	}
	decoded, err := transport.ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header.Magic != transport.Magic ||
		decoded.Header.HeaderLen != transport.HeaderLen ||
		decoded.Header.Version != 1 ||
		decoded.Header.Command != 42 ||
		decoded.Header.Status != transport.StatusOK ||
		string(decoded.Body) != "hello" {
		t.Fatalf("unexpected frame: %#v", decoded)
	}
}

func TestReadFrameWithLimitRejectsLargeBody(t *testing.T) {
	frame, err := transport.NewFrame(1, 42, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if writeErr := transport.WriteFrame(&buf, frame); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, readErr := transport.ReadFrameWithLimit(&buf, 4); readErr == nil {
		t.Fatal("expected body limit error")
	}
}
