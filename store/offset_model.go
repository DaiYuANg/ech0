package store

type ConsumerOffsetState struct {
	Consumer    string `json:"consumer"`
	Topic       string `json:"topic"`
	Partition   uint32 `json:"partition"`
	NextOffset  uint64 `json:"next_offset"`
	Metadata    string `json:"metadata,omitempty"`
	UpdatedAtMS uint64 `json:"updated_at_ms"`
}

func (s ConsumerOffsetState) TopicPartition() TopicPartition {
	return NewTopicPartition(s.Topic, s.Partition)
}
