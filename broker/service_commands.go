package broker

import "github.com/lyonbrown4d/ech0/store"

type produceCommand struct {
	Topic        string
	Partitioning PublishPartitioning
	Record       store.RecordAppend
	Idempotency  *ProduceIdempotency
}

type produceBatchCommand struct {
	Topic        string
	Partitioning PublishPartitioning
	Records      []store.RecordAppend
	Idempotency  *ProduceIdempotency
}

type produceBatchesCommand struct {
	Requests []produceBatchCommand
}

type produceBatchItemResult struct {
	Result   ProduceBatchResult
	Error    string
	Appended bool
}

type produceBatchesResult struct {
	Items []produceBatchItemResult
}

type commitOffsetCommand struct {
	Consumer   string
	Topic      string
	Partition  uint32
	NextOffset uint64
}

type commitOffsetsCommand struct {
	Requests []commitOffsetCommand
}

type commitOffsetItemResult struct {
	Error string
}

type commitOffsetsResult struct {
	Items []commitOffsetItemResult
}

type consumerPauseCommand struct {
	Consumer    string
	Topic       string
	Partition   uint32
	Paused      bool
	UpdatedAtMS uint64
}

type TransactionIdentity struct {
	TxID          uint64
	ProducerID    uint64
	ProducerEpoch uint64
}

type ProduceIdempotency struct {
	ProducerID    uint64
	ProducerEpoch uint64
	BaseSequence  uint64
}

type txBeginCommand struct {
	TransactionalID string
	TimeoutMS       uint64
}

type txPublishCommand struct {
	Identity  TransactionIdentity
	Sequence  uint64
	Topic     string
	Partition uint32
	Record    store.RecordAppend
}

type txPublishBatchCommand struct {
	Identity     TransactionIdentity
	BaseSequence uint64
	Topic        string
	Partition    uint32
	Records      []store.RecordAppend
}

type txCommitOffsetCommand struct {
	Identity   TransactionIdentity
	Consumer   string
	Group      string
	MemberID   string
	Generation uint64
	Topic      string
	Partition  uint32
	NextOffset uint64
}

type txBoundaryCommand struct {
	Identity TransactionIdentity
}

type txExpireCommand struct {
	NowMS uint64
}

type directCommand struct {
	Sender         string
	Recipient      string
	ConversationID *string
	Payload        []byte
}

type ackDirectCommand struct {
	Recipient  string
	NextOffset uint64
}

type joinGroupCommand struct {
	Group            string
	MemberID         string
	Topics           []string
	SessionTimeoutMS uint64
}

type heartbeatGroupCommand struct {
	Group            string
	MemberID         string
	SessionTimeoutMS *uint64
}

type rebalanceGroupCommand struct {
	Group string
}
