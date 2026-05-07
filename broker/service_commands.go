package broker

import "github.com/DaiYuANg/ech0/store"

type produceCommand struct {
	Topic        string
	Partitioning PublishPartitioning
	Key          []byte
	Tombstone    bool
	Payload      []byte
}

type produceBatchCommand struct {
	Topic        string
	Partitioning PublishPartitioning
	Records      []store.RecordAppend
}

type commitOffsetCommand struct {
	Consumer   string
	Topic      string
	Partition  uint32
	NextOffset uint64
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
