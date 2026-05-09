package protocol

type TransactionIdentity struct {
	TxID          uint64 `json:"tx_id"`
	ProducerID    uint64 `json:"producer_id"`
	ProducerEpoch uint64 `json:"producer_epoch"`
}

type TxBeginRequest struct {
	TransactionalID string `json:"transactional_id"`
	TimeoutMS       uint64 `json:"timeout_ms"`
}

type TxBeginResponse struct {
	Identity    TransactionIdentity `json:"identity"`
	ExpiresAtMS uint64              `json:"expires_at_ms"`
	Status      TransactionStatus   `json:"status"`
}

type TxPublishRequest struct {
	Identity     TransactionIdentity `json:"identity"`
	Sequence     uint64              `json:"sequence"`
	Topic        string              `json:"topic"`
	Partition    *uint32             `json:"partition,omitempty"`
	Partitioning ProducePartitioning `json:"partitioning"`
	Key          []byte              `json:"key,omitempty"`
	Headers      []MessageHeader     `json:"headers,omitempty"`
	Tombstone    bool                `json:"tombstone,omitempty"`
	Payload      []byte              `json:"payload"`
}

type TxPublishResponse struct {
	TxID       uint64 `json:"tx_id"`
	Partition  uint32 `json:"partition"`
	Offset     uint64 `json:"offset"`
	NextOffset uint64 `json:"next_offset"`
}

type TxPublishBatchRequest struct {
	Identity     TransactionIdentity  `json:"identity"`
	BaseSequence uint64               `json:"base_sequence"`
	Topic        string               `json:"topic"`
	Partition    *uint32              `json:"partition,omitempty"`
	Partitioning ProducePartitioning  `json:"partitioning"`
	Payloads     [][]byte             `json:"payloads,omitempty"`
	Records      []ProduceBatchRecord `json:"records,omitempty"`
}

type TxPublishBatchResponse struct {
	TxID       uint64 `json:"tx_id"`
	Partition  uint32 `json:"partition"`
	BaseOffset uint64 `json:"base_offset"`
	LastOffset uint64 `json:"last_offset"`
	NextOffset uint64 `json:"next_offset"`
	Appended   int    `json:"appended"`
}

type TxCommitOffsetRequest struct {
	Identity   TransactionIdentity `json:"identity"`
	Consumer   string              `json:"consumer,omitempty"`
	Group      string              `json:"group,omitempty"`
	MemberID   string              `json:"member_id,omitempty"`
	Generation uint64              `json:"generation,omitempty"`
	Topic      string              `json:"topic"`
	Partition  uint32              `json:"partition"`
	NextOffset uint64              `json:"next_offset"`
}

type TxCommitOffsetResponse struct {
	TxID       uint64 `json:"tx_id"`
	Consumer   string `json:"consumer,omitempty"`
	Group      string `json:"group,omitempty"`
	Topic      string `json:"topic"`
	Partition  uint32 `json:"partition"`
	NextOffset uint64 `json:"next_offset"`
}

type TxCommitRequest struct {
	Identity TransactionIdentity `json:"identity"`
}

type TxCommitResponse struct {
	TxID   uint64            `json:"tx_id"`
	Status TransactionStatus `json:"status"`
}

type TxAbortRequest struct {
	Identity TransactionIdentity `json:"identity"`
}

type TxAbortResponse struct {
	TxID   uint64            `json:"tx_id"`
	Status TransactionStatus `json:"status"`
}
