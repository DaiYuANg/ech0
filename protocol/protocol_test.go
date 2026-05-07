package protocol

import "testing"

func TestHandshakeJSONRoundTrip(t *testing.T) {
	req := HandshakeRequest{ClientID: "client-1"}
	data, err := EncodeJSON(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeJSON[HandshakeRequest](data)
	if err != nil {
		t.Fatal(err)
	}
	if got != req {
		t.Fatalf("got %#v, want %#v", got, req)
	}
}

func TestCommandIDsAreUnique(t *testing.T) {
	commands := []uint16{
		CmdHandshakeRequest,
		CmdPingRequest,
		CmdCreateTopicRequest,
		CmdProduceRequest,
		CmdFetchRequest,
		CmdCommitOffsetRequest,
		CmdListTopicsRequest,
		CmdSendDirectRequest,
		CmdFetchInboxRequest,
		CmdAckDirectRequest,
		CmdJoinConsumerGroupRequest,
		CmdHeartbeatConsumerGroupRequest,
		CmdRebalanceConsumerGroupRequest,
		CmdGetConsumerGroupAssignmentRequest,
		CmdProduceBatchRequest,
		CmdFetchBatchRequest,
		CmdNackRequest,
		CmdProcessRetryRequest,
		CmdScheduleDelayRequest,
		CmdFetchConsumerGroupRequest,
		CmdCommitConsumerGroupOffsetRequest,
		CmdFetchConsumerGroupBatchRequest,
		CmdHandshakeResponse,
		CmdPingResponse,
		CmdCreateTopicResponse,
		CmdProduceResponse,
		CmdFetchResponse,
		CmdCommitOffsetResponse,
		CmdListTopicsResponse,
		CmdSendDirectResponse,
		CmdFetchInboxResponse,
		CmdAckDirectResponse,
		CmdJoinConsumerGroupResponse,
		CmdHeartbeatConsumerGroupResponse,
		CmdRebalanceConsumerGroupResponse,
		CmdGetConsumerGroupAssignmentResponse,
		CmdProduceBatchResponse,
		CmdFetchBatchResponse,
		CmdNackResponse,
		CmdProcessRetryResponse,
		CmdScheduleDelayResponse,
		CmdFetchConsumerGroupResponse,
		CmdCommitConsumerGroupOffsetResponse,
		CmdFetchConsumerGroupBatchResponse,
		CmdErrorResponse,
	}
	seen := map[uint16]struct{}{}
	for _, command := range commands {
		if _, ok := seen[command]; ok {
			t.Fatalf("duplicate command id %d", command)
		}
		seen[command] = struct{}{}
	}
}
