package broker

import collectionlist "github.com/arcgolabs/collectionx/list"

func encodeBinaryCommitOffsetsCommand(req commitOffsetsCommand) ([]byte, error) {
	writer := newRaftBinaryWriter()
	count, err := checkedRaftUint32(len(req.Requests), "commit offset requests")
	if err != nil {
		return nil, err
	}
	writer.writeU32(count)
	for _, item := range req.Requests {
		if err := writeBinaryCommitOffsetCommand(writer, item); err != nil {
			return nil, err
		}
	}
	return writer.bytes(), nil
}

func writeBinaryCommitOffsetCommand(writer *raftBinaryWriter, item commitOffsetCommand) error {
	if err := writer.writeString(item.Consumer); err != nil {
		return err
	}
	if err := writer.writeString(item.Topic); err != nil {
		return err
	}
	writer.writeU32(item.Partition)
	writer.writeU64(item.NextOffset)
	writer.writeU64(item.UpdatedAtMS)
	return writer.writeString(item.Metadata)
}

func decodeBinaryCommitOffsetsCommand(data []byte) (commitOffsetsCommand, error) {
	reader := newRaftBinaryReader(data)
	requests, err := readBinaryCommitOffsetCommands(reader)
	if err != nil {
		return commitOffsetsCommand{}, err
	}
	if err := reader.ensureEOF(); err != nil {
		return commitOffsetsCommand{}, err
	}
	return commitOffsetsCommand{Requests: requests}, nil
}

func readBinaryCommitOffsetCommands(reader *raftBinaryReader) ([]commitOffsetCommand, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromRaftUint32(count)
	if err != nil {
		return nil, err
	}
	requests := collectionlist.NewListWithCapacity[commitOffsetCommand](size)
	for range size {
		item, err := readBinaryCommitOffsetCommand(reader)
		if err != nil {
			return nil, err
		}
		requests.Add(item)
	}
	return requests.Values(), nil
}

func readBinaryCommitOffsetCommand(reader *raftBinaryReader) (commitOffsetCommand, error) {
	consumer, err := reader.readString()
	if err != nil {
		return commitOffsetCommand{}, err
	}
	topic, err := reader.readString()
	if err != nil {
		return commitOffsetCommand{}, err
	}
	partition, err := reader.readU32()
	if err != nil {
		return commitOffsetCommand{}, err
	}
	nextOffset, err := reader.readU64()
	if err != nil {
		return commitOffsetCommand{}, err
	}
	updatedAtMS, err := reader.readU64()
	if err != nil {
		return commitOffsetCommand{}, err
	}
	metadata, err := reader.readString()
	if err != nil {
		return commitOffsetCommand{}, err
	}
	return commitOffsetCommand{
		Consumer:    consumer,
		Topic:       topic,
		Partition:   partition,
		NextOffset:  nextOffset,
		UpdatedAtMS: updatedAtMS,
		Metadata:    metadata,
	}, nil
}
