package protocol

import "github.com/samber/oops"

func writeTopicOrderingPolicy(writer *binaryWriter, value *TopicOrderingPolicy) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	switch *value {
	case TopicOrderingPartition:
		writer.writeU8(1)
	case TopicOrderingKey:
		writer.writeU8(2)
	case TopicOrderingRoutingKey:
		writer.writeU8(3)
	default:
		writer.writeU8(0)
	}
}

func readTopicOrderingPolicy(reader *binaryReader) (*TopicOrderingPolicy, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := reader.readU8()
	if err != nil {
		return nil, err
	}
	var out TopicOrderingPolicy
	switch value {
	case 1:
		out = TopicOrderingPartition
	case 2:
		out = TopicOrderingKey
	case 3:
		out = TopicOrderingRoutingKey
	default:
		return nil, oops.In("protocol").Code("binary_unknown_topic_ordering_policy").With("value", value).New("unknown topic ordering policy")
	}
	return &out, nil
}
