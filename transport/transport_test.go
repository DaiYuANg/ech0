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
	if err := WriteFrame(&buf, frame); err != nil {
		t.Fatal(err)
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
	if err := WriteFrame(&buf, frame); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrameWithLimit(&buf, 4); err == nil {
		t.Fatal("expected body limit error")
	}
}
