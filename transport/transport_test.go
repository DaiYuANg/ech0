//nolint:testpackage // Same-package tests keep transport frame helpers local.
package transport

import (
	"bytes"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	frame, err := NewFrame(1, 42, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if writeErr := WriteFrame(&buf, frame); writeErr != nil {
		t.Fatal(writeErr)
	}
	decoded, err := ReadFrame(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header.Version != 1 || decoded.Header.Command != 42 || string(decoded.Body) != "hello" {
		t.Fatalf("unexpected frame: %#v", decoded)
	}
}

func TestReadFrameWithLimitRejectsLargeBody(t *testing.T) {
	frame, err := NewFrame(1, 42, []byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if writeErr := WriteFrame(&buf, frame); writeErr != nil {
		t.Fatal(writeErr)
	}
	if _, readErr := ReadFrameWithLimit(&buf, 4); readErr == nil {
		t.Fatal("expected body limit error")
	}
}
