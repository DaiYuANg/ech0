package broker

type TopicCreatedEvent struct {
	Topic      string
	Partitions uint32
}

func (TopicCreatedEvent) Name() string { return "ech0.topic.created" }

type RecordProducedEvent struct {
	Topic      string
	Partition  uint32
	Offset     uint64
	NextOffset uint64
}

func (RecordProducedEvent) Name() string { return "ech0.record.produced" }

type DirectMessageSentEvent struct {
	Sender    string
	Recipient string
	Offset    uint64
}

func (DirectMessageSentEvent) Name() string { return "ech0.direct.sent" }
