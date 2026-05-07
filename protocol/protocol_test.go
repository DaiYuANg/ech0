package protocol_test

import (
	"testing"

	protocol "github.com/DaiYuANg/ech0/protocol"
	collectionset "github.com/arcgolabs/collectionx/set"
)

func TestHandshakeBinaryRoundTrip(t *testing.T) {
	req := protocol.HandshakeRequest{ClientID: "client-1"}
	data, err := protocol.EncodeBody(protocol.CmdHandshakeRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.HandshakeRequest
	if err := protocol.DecodeBody(protocol.CmdHandshakeRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if got != req {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestCommandIDsAreUnique(t *testing.T) {
	commands := protocol.CommandIDs()
	seen := collectionset.NewSet[uint16]()
	for _, command := range commands {
		if seen.Contains(command) {
			t.Fatalf("duplicate command id %d", command)
		}
		seen.Add(command)
	}
}
