package protocol_test

import (
	"reflect"
	"testing"

	collectionset "github.com/arcgolabs/collectionx/set"
	protocol "github.com/lyonbrown4d/ech0/protocol"
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

func TestProtocolVersionStaysV1BeforeRelease(t *testing.T) {
	if protocol.Version != 1 {
		t.Fatalf("protocol version = %d, want 1", protocol.Version)
	}
}

func TestTransactionBeginBinaryRoundTrip(t *testing.T) {
	req := protocol.TxBeginRequest{TransactionalID: "orders-worker-1", TimeoutMS: 30_000}
	data, err := protocol.EncodeBody(protocol.CmdTxBeginRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.TxBeginRequest
	if err := protocol.DecodeBody(protocol.CmdTxBeginRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if got != req {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestTransactionPublishBatchBinaryRoundTrip(t *testing.T) {
	partition := uint32(3)
	req := protocol.TxPublishBatchRequest{
		Identity:     protocol.TransactionIdentity{TxID: 10, ProducerID: 20, ProducerEpoch: 2},
		BaseSequence: 42,
		Topic:        "orders",
		Partition:    &partition,
		Partitioning: protocol.ProducePartitioningExplicit,
		Records: []protocol.ProduceBatchRecord{
			{Key: []byte("a"), Headers: []protocol.MessageHeader{{Key: "trace", Value: []byte("1")}}, Payload: []byte("payload-1")},
			{Key: []byte("b"), Tombstone: true, Payload: []byte{}},
		},
	}
	data, err := protocol.EncodeBody(protocol.CmdTxPublishBatchRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.TxPublishBatchRequest
	if err := protocol.DecodeBody(protocol.CmdTxPublishBatchRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestFetchResponseTransactionMetadataBinaryRoundTrip(t *testing.T) {
	resp := protocol.FetchResponse{
		Topic:     "orders",
		Partition: 1,
		Records: []protocol.FetchRecord{{
			Offset:      7,
			TimestampMS: 123,
			Key:         []byte{},
			Transaction: &protocol.TransactionRecordMetadata{
				TxID: 10, ProducerID: 20, ProducerEpoch: 2, Sequence: 43,
				ControlType: protocol.TransactionControlCommit,
			},
			Payload: []byte("ok"),
		}},
		NextOffset: 8,
	}
	data, err := protocol.EncodeBody(protocol.CmdFetchResponse, resp)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.FetchResponse
	if err := protocol.DecodeBody(protocol.CmdFetchResponse, data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, resp) {
		t.Fatalf("got %#v, want %#v", got, resp)
	}
}

func TestFetchIsolationBinaryRoundTrip(t *testing.T) {
	req := protocol.FetchRequest{
		Consumer: "c1", Topic: "orders", Partition: 0,
		MaxRecords: 10, Isolation: protocol.FetchIsolationReadCommitted,
	}
	data, err := protocol.EncodeBody(protocol.CmdFetchRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.FetchRequest
	if err := protocol.DecodeBody(protocol.CmdFetchRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, req) {
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
