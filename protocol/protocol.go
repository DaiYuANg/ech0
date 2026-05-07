// Package protocol defines the ech0 wire protocol messages.
package protocol

// Version is the current ech0 wire protocol version.
const Version uint8 = 2

const (
	CmdHandshakeRequest uint16 = 1
	CmdPingRequest      uint16 = 2

	CmdCreateTopicRequest   uint16 = 10
	CmdListTopicsRequest    uint16 = 11
	CmdProduceRequest       uint16 = 20
	CmdProduceBatchRequest  uint16 = 21
	CmdFetchRequest         uint16 = 22
	CmdFetchBatchRequest    uint16 = 23
	CmdCommitOffsetRequest  uint16 = 24
	CmdNackRequest          uint16 = 30
	CmdProcessRetryRequest  uint16 = 31
	CmdScheduleDelayRequest uint16 = 32

	CmdSendDirectRequest uint16 = 40
	CmdFetchInboxRequest uint16 = 41
	CmdAckDirectRequest  uint16 = 42

	CmdStartRequestRequest  uint16 = 50
	CmdFetchRequestsRequest uint16 = 51
	CmdReplyRequest         uint16 = 52
	CmdReplyErrorRequest    uint16 = 53
	CmdAwaitReplyRequest    uint16 = 54

	CmdJoinConsumerGroupRequest          uint16 = 60
	CmdHeartbeatConsumerGroupRequest     uint16 = 61
	CmdRebalanceConsumerGroupRequest     uint16 = 62
	CmdGetConsumerGroupAssignmentRequest uint16 = 63
	CmdFetchConsumerGroupRequest         uint16 = 64
	CmdCommitConsumerGroupOffsetRequest  uint16 = 65
	CmdFetchConsumerGroupBatchRequest    uint16 = 66
)

const (
	CmdHandshakeResponse     uint16 = 1001
	CmdPingResponse          uint16 = 1002
	CmdCreateTopicResponse   uint16 = 1010
	CmdListTopicsResponse    uint16 = 1011
	CmdProduceResponse       uint16 = 1020
	CmdProduceBatchResponse  uint16 = 1021
	CmdFetchResponse         uint16 = 1022
	CmdFetchBatchResponse    uint16 = 1023
	CmdCommitOffsetResponse  uint16 = 1024
	CmdNackResponse          uint16 = 1030
	CmdProcessRetryResponse  uint16 = 1031
	CmdScheduleDelayResponse uint16 = 1032

	CmdSendDirectResponse uint16 = 1040
	CmdFetchInboxResponse uint16 = 1041
	CmdAckDirectResponse  uint16 = 1042

	CmdStartRequestResponse  uint16 = 1050
	CmdFetchRequestsResponse uint16 = 1051
	CmdReplyResponse         uint16 = 1052
	CmdReplyErrorResponse    uint16 = 1053
	CmdAwaitReplyResponse    uint16 = 1054

	CmdJoinConsumerGroupResponse          uint16 = 1060
	CmdHeartbeatConsumerGroupResponse     uint16 = 1061
	CmdRebalanceConsumerGroupResponse     uint16 = 1062
	CmdGetConsumerGroupAssignmentResponse uint16 = 1063
	CmdFetchConsumerGroupResponse         uint16 = 1064
	CmdCommitConsumerGroupOffsetResponse  uint16 = 1065
	CmdFetchConsumerGroupBatchResponse    uint16 = 1066
	CmdErrorResponse                      uint16 = 1500
)

// CommandIDs returns every command id defined by the current wire protocol.
func CommandIDs() []uint16 {
	return registeredCommandIDs()
}

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
