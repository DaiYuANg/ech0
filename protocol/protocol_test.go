package protocol_test

import (
	"testing"

	protocol "github.com/DaiYuANg/ech0/protocol"
	collectionset "github.com/arcgolabs/collectionx/set"
)

func TestHandshakeJSONRoundTrip(t *testing.T) {
	req := protocol.HandshakeRequest{ClientID: "client-1"}
	data, err := protocol.EncodeJSON(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := protocol.DecodeJSON[protocol.HandshakeRequest](data)
	if err != nil {
		t.Fatal(err)
	}
	if got != req {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestCommandIDsAreUnique(t *testing.T) {
	commands := []uint16{
		protocol.CmdHandshakeRequest,
		protocol.CmdPingRequest,
		protocol.CmdCreateTopicRequest,
		protocol.CmdProduceRequest,
		protocol.CmdFetchRequest,
		protocol.CmdCommitOffsetRequest,
		protocol.CmdListTopicsRequest,
		protocol.CmdSendDirectRequest,
		protocol.CmdFetchInboxRequest,
		protocol.CmdAckDirectRequest,
		protocol.CmdJoinConsumerGroupRequest,
		protocol.CmdHeartbeatConsumerGroupRequest,
		protocol.CmdRebalanceConsumerGroupRequest,
		protocol.CmdGetConsumerGroupAssignmentRequest,
		protocol.CmdProduceBatchRequest,
		protocol.CmdFetchBatchRequest,
		protocol.CmdNackRequest,
		protocol.CmdProcessRetryRequest,
		protocol.CmdScheduleDelayRequest,
		protocol.CmdFetchConsumerGroupRequest,
		protocol.CmdCommitConsumerGroupOffsetRequest,
		protocol.CmdFetchConsumerGroupBatchRequest,
		protocol.CmdHandshakeResponse,
		protocol.CmdPingResponse,
		protocol.CmdCreateTopicResponse,
		protocol.CmdProduceResponse,
		protocol.CmdFetchResponse,
		protocol.CmdCommitOffsetResponse,
		protocol.CmdListTopicsResponse,
		protocol.CmdSendDirectResponse,
		protocol.CmdFetchInboxResponse,
		protocol.CmdAckDirectResponse,
		protocol.CmdJoinConsumerGroupResponse,
		protocol.CmdHeartbeatConsumerGroupResponse,
		protocol.CmdRebalanceConsumerGroupResponse,
		protocol.CmdGetConsumerGroupAssignmentResponse,
		protocol.CmdProduceBatchResponse,
		protocol.CmdFetchBatchResponse,
		protocol.CmdNackResponse,
		protocol.CmdProcessRetryResponse,
		protocol.CmdScheduleDelayResponse,
		protocol.CmdFetchConsumerGroupResponse,
		protocol.CmdCommitConsumerGroupOffsetResponse,
		protocol.CmdFetchConsumerGroupBatchResponse,
		protocol.CmdErrorResponse,
	}
	seen := collectionset.NewSet[uint16]()
	for _, command := range commands {
		if seen.Contains(command) {
			t.Fatalf("duplicate command id %d", command)
		}
		seen.Add(command)
	}
}
