package broker

const (
	raftCommandCreateTopic    = "create_topic"
	raftCommandProduce        = "produce"
	raftCommandProduceBatch   = "produce_batch"
	raftCommandProduceBatches = "produce_batches"
	raftCommandCommitOffset   = "commit_offset"
	raftCommandCommitOffsets  = "commit_offsets"
	raftCommandTxBegin        = "tx_begin"
	raftCommandTxPublish      = "tx_publish"
	raftCommandTxPublishBatch = "tx_publish_batch"
	raftCommandTxCommitOffset = "tx_commit_offset"
	raftCommandTxCommit       = "tx_commit"
	raftCommandTxAbort        = "tx_abort"
	raftCommandDirectSend     = "direct_send"
	raftCommandDirectAck      = "direct_ack"
	raftCommandJoinGroup      = "join_group"
	raftCommandHeartbeatGroup = "heartbeat_group"
	raftCommandRebalanceGroup = "rebalance_group"
)

type raftCommand struct {
	Type    string         `json:"type"`
	Payload jsonRawMessage `json:"payload"`
}
