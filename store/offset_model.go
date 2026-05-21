package store

type ConsumerOffsetState struct {
	Consumer       string   `json:"consumer"`
	Topic          string   `json:"topic"`
	Partition      uint32   `json:"partition"`
	NextOffset     uint64   `json:"next_offset"`
	PendingOffsets []uint64 `json:"pending_offsets,omitempty"`
	Metadata       string   `json:"metadata,omitempty"`
	UpdatedAtMS    uint64   `json:"updated_at_ms"`
}

func (s ConsumerOffsetState) TopicPartition() TopicPartition {
	return NewTopicPartition(s.Topic, s.Partition)
}

type PartitionOffsetState struct {
	Topic           string  `json:"topic"`
	Partition       uint32  `json:"partition"`
	LogStartOffset  uint64  `json:"log_start_offset"`
	LowWatermark    *uint64 `json:"low_watermark,omitempty"`
	HighWatermark   *uint64 `json:"high_watermark,omitempty"`
	NextOffset      uint64  `json:"next_offset"`
	RetainedRecords uint64  `json:"retained_records"`
}

func (s PartitionOffsetState) TopicPartition() TopicPartition {
	return NewTopicPartition(s.Topic, s.Partition)
}

func partitionOffsetsFromRecordOffsets(tp TopicPartition, nextOffset uint64, offsets []uint64) PartitionOffsetState {
	state := PartitionOffsetState{
		Topic:           tp.Topic,
		Partition:       tp.Partition,
		LogStartOffset:  nextOffset,
		NextOffset:      nextOffset,
		RetainedRecords: uint64(len(offsets)),
	}
	if nextOffset > 0 {
		value := nextOffset - 1
		state.HighWatermark = &value
	}
	if len(offsets) > 0 {
		value := offsets[0]
		state.LowWatermark = &value
		state.LogStartOffset = value
	}
	return state
}

func recordOffsets(records []Record) []uint64 {
	if len(records) == 0 {
		return nil
	}
	out := make([]uint64, 0, len(records))
	for _, record := range records {
		out = append(out, record.Offset)
	}
	return out
}
