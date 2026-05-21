package protocol

func writeTopicPriorityPolicy(writer *binaryWriter, value *TopicPriorityPolicy) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeBool(value.Enabled)
	writer.writeU8(value.Min)
	writer.writeU8(value.Max)
	writer.writeU8(value.Default)
}

func readTopicPriorityPolicy(reader *binaryReader) (*TopicPriorityPolicy, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return nil, err
	}
	enabled, err := reader.readBool()
	if err != nil {
		return nil, err
	}
	minPriority, err := reader.readU8()
	if err != nil {
		return nil, err
	}
	maxPriority, err := reader.readU8()
	if err != nil {
		return nil, err
	}
	defaultPriority, err := reader.readU8()
	if err != nil {
		return nil, err
	}
	return &TopicPriorityPolicy{
		Enabled: enabled,
		Min:     minPriority,
		Max:     maxPriority,
		Default: defaultPriority,
	}, nil
}
