package protocol_test

import (
	"reflect"
	"testing"

	protocol "github.com/lyonbrown4d/ech0/protocol"
)

func TestProduceFanoutBinaryRoundTrip(t *testing.T) {
	expiresAt := uint64(1700000005000)
	req := protocol.ProduceFanoutRequest{
		Topic:       "events",
		RoutingKey:  "tenant.updated",
		Key:         []byte("tenant-1"),
		Headers:     []protocol.MessageHeader{{Key: "trace", Value: []byte("1")}},
		ExpiresAtMS: &expiresAt,
		Payload:     []byte("broadcast"),
	}
	data, err := protocol.EncodeBody(protocol.CmdProduceFanoutRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.ProduceFanoutRequest
	if decodeErr := protocol.DecodeBody(protocol.CmdProduceFanoutRequest, data, &got); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
	}

	resp := protocol.ProduceFanoutResponse{Records: []protocol.ProduceFanoutRecordResponse{{Partition: 1, Offset: 2, NextOffset: 3}}}
	encoded, err := protocol.EncodeBody(protocol.CmdProduceFanoutResponse, resp)
	if err != nil {
		t.Fatal(err)
	}
	var decoded protocol.ProduceFanoutResponse
	if err := protocol.DecodeBody(protocol.CmdProduceFanoutResponse, encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, resp) {
		t.Fatalf("got %#v, want %#v", decoded, resp)
	}
}

func TestFetchSubjectPatternBinaryRoundTrip(t *testing.T) {
	req := protocol.FetchSubjectPatternRequest{
		Consumer:   "worker",
		Pattern:    "orders.*",
		MaxRecords: 10,
		Isolation:  protocol.FetchIsolationReadCommitted,
	}
	data, err := protocol.EncodeBody(protocol.CmdFetchSubjectPatternRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.FetchSubjectPatternRequest
	if decodeErr := protocol.DecodeBody(protocol.CmdFetchSubjectPatternRequest, data, &got); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
	}

	resp := protocol.FetchSubjectPatternResponse{
		Pattern: "orders.*",
		Items: []protocol.FetchSubjectPatternItemResponse{{
			Topic:      "orders.created",
			Partition:  0,
			Records:    []protocol.FetchRecord{{Offset: 1, TimestampMS: 2, Key: []byte{}, Payload: []byte("created")}},
			NextOffset: 2,
		}},
	}
	encoded, err := protocol.EncodeBody(protocol.CmdFetchSubjectPatternResponse, resp)
	if err != nil {
		t.Fatal(err)
	}
	var decoded protocol.FetchSubjectPatternResponse
	if err := protocol.DecodeBody(protocol.CmdFetchSubjectPatternResponse, encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded, resp) {
		t.Fatalf("got %#v, want %#v", decoded, resp)
	}
}
