package broker

import "github.com/DaiYuANg/ech0/store"

const defaultTransactionTimeoutMS uint64 = 60_000

type FetchIsolation string

const (
	FetchIsolationReadUncommitted FetchIsolation = "read_uncommitted"
	FetchIsolationReadCommitted   FetchIsolation = "read_committed"
)

type TransactionBeginResult struct {
	Identity    TransactionIdentity
	ExpiresAtMS uint64
	Status      store.TransactionStatus
}

type TransactionPublishResult struct {
	TxID       uint64
	Partition  uint32
	Record     store.Record
	NextOffset uint64
}

type TransactionPublishBatchResult struct {
	TxID       uint64
	Partition  uint32
	Records    []store.Record
	BaseOffset uint64
	LastOffset uint64
	NextOffset uint64
}

type TransactionOffsetCommit struct {
	Consumer   string
	Group      string
	MemberID   string
	Generation uint64
	Topic      string
	Partition  uint32
	NextOffset uint64
}

type TransactionOffsetCommitResult struct {
	TxID   uint64
	Offset TransactionOffsetCommit
}

type TransactionBoundaryResult struct {
	TxID   uint64
	Status store.TransactionStatus
}
