package protocol

func encodeCommitConsumerGroupOffsetRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req CommitConsumerGroupOffsetRequest) error {
		return writeGroupOffset(writer, groupOffsetFields{
			group: req.Group, memberID: req.MemberID, generation: req.Generation,
			topic: req.Topic, partition: req.Partition, nextOffset: req.NextOffset, metadata: req.Metadata,
		})
	})
}

func decodeCommitConsumerGroupOffsetRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (CommitConsumerGroupOffsetRequest, error) {
		fields, err := readGroupOffset(reader)
		return CommitConsumerGroupOffsetRequest{
			Group: fields.group, MemberID: fields.memberID, Generation: fields.generation,
			Topic: fields.topic, Partition: fields.partition, NextOffset: fields.nextOffset, Metadata: fields.metadata,
		}, err
	})
}

func encodeCommitConsumerGroupOffsetResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp CommitConsumerGroupOffsetResponse) error {
		return writeGroupOffset(writer, groupOffsetFields{
			group: resp.Group, memberID: resp.MemberID, generation: resp.Generation,
			topic: resp.Topic, partition: resp.Partition, nextOffset: resp.NextOffset, metadata: resp.Metadata,
		})
	})
}

func decodeCommitConsumerGroupOffsetResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (CommitConsumerGroupOffsetResponse, error) {
		fields, err := readGroupOffset(reader)
		return CommitConsumerGroupOffsetResponse{
			Group: fields.group, MemberID: fields.memberID, Generation: fields.generation,
			Topic: fields.topic, Partition: fields.partition, NextOffset: fields.nextOffset, Metadata: fields.metadata,
		}, err
	})
}

type groupOffsetFields struct {
	group      string
	memberID   string
	generation uint64
	topic      string
	partition  uint32
	nextOffset uint64
	metadata   string
}

func writeGroupOffset(writer *binaryWriter, fields groupOffsetFields) error {
	if err := writer.writeString(fields.group); err != nil {
		return err
	}
	if err := writer.writeString(fields.memberID); err != nil {
		return err
	}
	writer.writeU64(fields.generation)
	if err := writer.writeString(fields.topic); err != nil {
		return err
	}
	writer.writeU32(fields.partition)
	writer.writeU64(fields.nextOffset)
	return writer.writeString(fields.metadata)
}

func readGroupOffset(reader *binaryReader) (groupOffsetFields, error) {
	group, err := reader.readString()
	if err != nil {
		return groupOffsetFields{}, err
	}
	memberID, err := reader.readString()
	if err != nil {
		return groupOffsetFields{}, err
	}
	generation, err := reader.readU64()
	if err != nil {
		return groupOffsetFields{}, err
	}
	return readGroupOffsetTail(reader, groupOffsetFields{group: group, memberID: memberID, generation: generation})
}

func readGroupOffsetTail(reader *binaryReader, fields groupOffsetFields) (groupOffsetFields, error) {
	topic, err := reader.readString()
	if err != nil {
		return groupOffsetFields{}, err
	}
	partition, err := reader.readU32()
	if err != nil {
		return groupOffsetFields{}, err
	}
	nextOffset, err := reader.readU64()
	if err != nil {
		return groupOffsetFields{}, err
	}
	metadata, err := reader.readString()
	fields.topic = topic
	fields.partition = partition
	fields.nextOffset = nextOffset
	fields.metadata = metadata
	return fields, err
}

func encodeFetchConsumerGroupBatchRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req FetchConsumerGroupBatchRequest) error {
		if err := writer.writeString(req.Group); err != nil {
			return err
		}
		if err := writer.writeString(req.MemberID); err != nil {
			return err
		}
		writer.writeU64(req.Generation)
		if err := writeFetchBatchItems(writer, req.Items); err != nil {
			return err
		}
		if err := writer.writeOptionalInt(req.MinRecords); err != nil {
			return err
		}
		writer.writeOptionalU64(req.MaxWaitMS)
		writeFetchIsolation(writer, req.Isolation)
		return nil
	})
}

func decodeFetchConsumerGroupBatchRequest(data []byte, target any) error {
	return decodeWith(data, target, readFetchConsumerGroupBatchRequest)
}

func readFetchConsumerGroupBatchRequest(reader *binaryReader) (FetchConsumerGroupBatchRequest, error) {
	group, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupBatchRequest{}, err
	}
	memberID, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupBatchRequest{}, err
	}
	req := FetchConsumerGroupBatchRequest{Group: group, MemberID: memberID}
	if req.Generation, err = reader.readU64(); err != nil {
		return FetchConsumerGroupBatchRequest{}, err
	}
	if req.Items, err = readFetchBatchItems(reader); err != nil {
		return FetchConsumerGroupBatchRequest{}, err
	}
	if req.MinRecords, err = reader.readOptionalInt(); err != nil {
		return FetchConsumerGroupBatchRequest{}, err
	}
	if req.MaxWaitMS, err = reader.readOptionalU64(); err != nil {
		return FetchConsumerGroupBatchRequest{}, err
	}
	if req.Isolation, err = readFetchIsolation(reader); err != nil {
		return FetchConsumerGroupBatchRequest{}, err
	}
	return req, nil
}
