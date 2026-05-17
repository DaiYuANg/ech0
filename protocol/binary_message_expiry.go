package protocol

import "github.com/samber/oops"

func writeMessageExpiryAction(writer *binaryWriter, value *MessageExpiryAction) {
	if value == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	switch *value {
	case MessageExpiryDelete:
		writer.writeU8(1)
	case MessageExpiryDLQ:
		writer.writeU8(2)
	default:
		writer.writeU8(0)
	}
}

func readMessageExpiryAction(reader *binaryReader) (*MessageExpiryAction, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return noMessageExpiryAction(), err
	}
	value, err := reader.readU8()
	if err != nil {
		return nil, err
	}
	var out MessageExpiryAction
	switch value {
	case 1:
		out = MessageExpiryDelete
	case 2:
		out = MessageExpiryDLQ
	default:
		return nil, oops.In("protocol").Code("binary_unknown_message_expiry_action").With("value", value).New("unknown message expiry action")
	}
	return &out, nil
}

func noMessageExpiryAction() *MessageExpiryAction {
	return nil
}
