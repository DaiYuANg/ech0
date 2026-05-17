package protocol_test

import (
	"reflect"
	"testing"

	collectionset "github.com/arcgolabs/collectionx/set"
	protocol "github.com/lyonbrown4d/ech0/protocol"
)

func TestHandshakeBinaryRoundTrip(t *testing.T) {
	req := protocol.HandshakeRequest{
		ClientID:     "client-1",
		Tenant:       "tenant-a",
		Namespace:    "payments",
		Principal:    "svc-a",
		AuthToken:    "token",
		Capabilities: []string{protocol.CapabilityCompressionZstd, protocol.CapabilityTransactions},
	}
	data, err := protocol.EncodeBody(protocol.CmdHandshakeRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.HandshakeRequest
	if err := protocol.DecodeBody(protocol.CmdHandshakeRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestCapabilityNegotiation(t *testing.T) {
	got := protocol.NegotiateCapabilities([]string{
		protocol.CapabilityTransactions,
		"unknown",
		protocol.CapabilityCompressionZstd,
		protocol.CapabilityTransactions,
	})
	want := []string{protocol.CapabilityCompressionZstd, protocol.CapabilityTransactions}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
	if len(protocol.NegotiateCapabilities(nil)) != len(protocol.SupportedCapabilities()) {
		t.Fatalf("empty request should return supported capabilities")
	}
}

func TestProtocolVersionStaysV1BeforeRelease(t *testing.T) {
	if protocol.Version != 1 {
		t.Fatalf("protocol version = %d, want 1", protocol.Version)
	}
}

func TestCreateTopicMessageExpiryBinaryRoundTrip(t *testing.T) {
	retentionMS := uint64(60_000)
	messageTTLMS := uint64(5_000)
	action := protocol.MessageExpiryDLQ
	req := protocol.CreateTopicRequest{
		Topic:               "orders",
		Partitions:          3,
		RetentionMS:         &retentionMS,
		MessageTTLMS:        &messageTTLMS,
		MessageExpiryAction: &action,
	}
	data, err := protocol.EncodeBody(protocol.CmdCreateTopicRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.CreateTopicRequest
	if err := protocol.DecodeBody(protocol.CmdCreateTopicRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestProduceMessageExpiryBinaryRoundTrip(t *testing.T) {
	expiresAt := uint64(1700000005000)
	req := protocol.ProduceRequest{
		Topic:        "orders",
		Partitioning: protocol.ProducePartitioningRoundRobin,
		Key:          []byte("k1"),
		ExpiresAtMS:  &expiresAt,
		Payload:      []byte("payload"),
	}
	data, err := protocol.EncodeBody(protocol.CmdProduceRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.ProduceRequest
	if err := protocol.DecodeBody(protocol.CmdProduceRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, req) {
		t.Fatalf("got %#v, want %#v", got, req)
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

func TestCommitOffsetMetadataBinaryRoundTrip(t *testing.T) {
	req := protocol.CommitOffsetRequest{
		Consumer: "c1", Topic: "orders", Partition: 0, NextOffset: 7, Metadata: "checkpoint=42",
	}
	data, err := protocol.EncodeBody(protocol.CmdCommitOffsetRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.CommitOffsetRequest
	if err := protocol.DecodeBody(protocol.CmdCommitOffsetRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if got != req {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestCommitConsumerGroupOffsetMetadataBinaryRoundTrip(t *testing.T) {
	req := protocol.CommitConsumerGroupOffsetRequest{
		Group: "workers", MemberID: "member-1", Generation: 2,
		Topic: "orders", Partition: 0, NextOffset: 7, Metadata: "checkpoint=42",
	}
	data, err := protocol.EncodeBody(protocol.CmdCommitConsumerGroupOffsetRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.CommitConsumerGroupOffsetRequest
	if err := protocol.DecodeBody(protocol.CmdCommitConsumerGroupOffsetRequest, data, &got); err != nil {
		t.Fatal(err)
	}
	if got != req {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestTransactionCommitOffsetMetadataBinaryRoundTrip(t *testing.T) {
	req := protocol.TxCommitOffsetRequest{
		Identity: protocol.TransactionIdentity{TxID: 10, ProducerID: 20, ProducerEpoch: 2},
		Consumer: "c1", Topic: "orders", Partition: 0, NextOffset: 7, Metadata: "checkpoint=42",
	}
	data, err := protocol.EncodeBody(protocol.CmdTxCommitOffsetRequest, req)
	if err != nil {
		t.Fatal(err)
	}
	var got protocol.TxCommitOffsetRequest
	if err := protocol.DecodeBody(protocol.CmdTxCommitOffsetRequest, data, &got); err != nil {
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
