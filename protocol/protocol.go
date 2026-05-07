// Package protocol defines the ech0 wire protocol messages.
package protocol

import (
	"encoding/json"

	"github.com/samber/oops"
)

const Version1 uint8 = 1

const (
	CmdHandshakeRequest                  uint16 = 1
	CmdPingRequest                       uint16 = 2
	CmdCreateTopicRequest                uint16 = 3
	CmdProduceRequest                    uint16 = 4
	CmdFetchRequest                      uint16 = 5
	CmdCommitOffsetRequest               uint16 = 6
	CmdListTopicsRequest                 uint16 = 7
	CmdSendDirectRequest                 uint16 = 8
	CmdFetchInboxRequest                 uint16 = 9
	CmdAckDirectRequest                  uint16 = 10
	CmdJoinConsumerGroupRequest          uint16 = 11
	CmdHeartbeatConsumerGroupRequest     uint16 = 12
	CmdRebalanceConsumerGroupRequest     uint16 = 13
	CmdGetConsumerGroupAssignmentRequest uint16 = 14
	CmdProduceBatchRequest               uint16 = 15
	CmdFetchBatchRequest                 uint16 = 16
	CmdNackRequest                       uint16 = 17
	CmdProcessRetryRequest               uint16 = 18
	CmdScheduleDelayRequest              uint16 = 19
	CmdFetchConsumerGroupRequest         uint16 = 20
	CmdCommitConsumerGroupOffsetRequest  uint16 = 21
	CmdFetchConsumerGroupBatchRequest    uint16 = 22
)

const (
	CmdHandshakeResponse                  uint16 = 101
	CmdPingResponse                       uint16 = 102
	CmdCreateTopicResponse                uint16 = 103
	CmdProduceResponse                    uint16 = 104
	CmdFetchResponse                      uint16 = 105
	CmdCommitOffsetResponse               uint16 = 106
	CmdListTopicsResponse                 uint16 = 107
	CmdSendDirectResponse                 uint16 = 108
	CmdFetchInboxResponse                 uint16 = 109
	CmdAckDirectResponse                  uint16 = 110
	CmdJoinConsumerGroupResponse          uint16 = 111
	CmdHeartbeatConsumerGroupResponse     uint16 = 112
	CmdRebalanceConsumerGroupResponse     uint16 = 113
	CmdGetConsumerGroupAssignmentResponse uint16 = 114
	CmdProduceBatchResponse               uint16 = 115
	CmdFetchBatchResponse                 uint16 = 116
	CmdNackResponse                       uint16 = 117
	CmdProcessRetryResponse               uint16 = 118
	CmdScheduleDelayResponse              uint16 = 119
	CmdFetchConsumerGroupResponse         uint16 = 120
	CmdCommitConsumerGroupOffsetResponse  uint16 = 121
	CmdFetchConsumerGroupBatchResponse    uint16 = 122
	CmdErrorResponse                      uint16 = 500
)

type TopicCleanupPolicy string

const (
	TopicCleanupDelete           TopicCleanupPolicy = "delete"
	TopicCleanupCompact          TopicCleanupPolicy = "compact"
	TopicCleanupCompactAndDelete TopicCleanupPolicy = "compact_and_delete"
)

type ProducePartitioning string

const (
	ProducePartitioningExplicit   ProducePartitioning = "explicit"
	ProducePartitioningRoundRobin ProducePartitioning = "round_robin"
	ProducePartitioningKeyHash    ProducePartitioning = "key_hash"
)

func EncodeJSON[T any](value T) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, oops.In("protocol").Code("json_encode_failed").Wrapf(err, "encode json")
	}
	return data, nil
}

func DecodeJSON[T any](data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		return value, oops.In("protocol").Code("json_decode_failed").Wrapf(err, "decode json")
	}
	return value, nil
}
