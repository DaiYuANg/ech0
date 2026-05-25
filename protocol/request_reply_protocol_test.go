package protocol_test

import (
	"testing"

	"github.com/lyonbrown4d/ech0/protocol"
	protocolbinary "github.com/lyonbrown4d/ech0/protocol/binary"
)

func TestRequestReplyModeBinaryRoundTrip(t *testing.T) {
	timeoutMS := uint64(1000)
	pollMS := uint64(1)
	partition := uint32(0)
	req := protocol.StartRequestRequest{
		Subject:        "svc.echo",
		InstanceID:     "A1",
		TimeoutMS:      &timeoutMS,
		PollIntervalMS: &pollMS,
		Partition:      &partition,
		Partitioning:   protocol.ProducePartitioningExplicit,
		ReplyMode:      protocol.RequestReplyModeMultiReplier,
		Payload:        []byte("ping"),
	}
	body, err := protocolbinary.EncodeBody(protocol.CmdStartRequestRequest, req)
	if err != nil {
		t.Fatal(err)
	}

	var got protocol.StartRequestRequest
	err = protocolbinary.DecodeBody(protocol.CmdStartRequestRequest, body, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got.ReplyMode != protocol.RequestReplyModeMultiReplier {
		t.Fatalf("unexpected reply mode %q", got.ReplyMode)
	}
}

func TestAwaitRepliesBinaryRoundTrip(t *testing.T) {
	pollMS := uint64(1)
	req := protocol.AwaitRepliesRequest{
		ReplyTo:        "__reply/A1",
		CorrelationID:  "corr-1",
		ExpiresAtMS:    42,
		PollIntervalMS: &pollMS,
		MaxReplies:     2,
	}
	body, err := protocolbinary.EncodeBody(protocol.CmdAwaitRepliesRequest, req)
	if err != nil {
		t.Fatal(err)
	}

	var got protocol.AwaitRepliesRequest
	err = protocolbinary.DecodeBody(protocol.CmdAwaitRepliesRequest, body, &got)
	if err != nil {
		t.Fatal(err)
	}
	if got.MaxReplies != 2 || got.ReplyTo != req.ReplyTo || got.CorrelationID != req.CorrelationID {
		t.Fatalf("unexpected await replies request: %#v", got)
	}

	resp := protocol.AwaitRepliesResponse{Replies: []protocol.ReplyRecord{
		{Offset: 1, MessageID: "m1", Subject: "svc.echo", CorrelationID: "corr-1", ResponderID: "B1", Payload: []byte("one")},
		{Offset: 2, MessageID: "m2", Subject: "svc.echo", CorrelationID: "corr-1", ResponderID: "B2", Payload: []byte("two")},
	}}
	body, err = protocolbinary.EncodeBody(protocol.CmdAwaitRepliesResponse, resp)
	if err != nil {
		t.Fatal(err)
	}

	var decoded protocol.AwaitRepliesResponse
	err = protocolbinary.DecodeBody(protocol.CmdAwaitRepliesResponse, body, &decoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Replies) != 2 || decoded.Replies[1].ResponderID != "B2" {
		t.Fatalf("unexpected await replies response: %#v", decoded)
	}
}
