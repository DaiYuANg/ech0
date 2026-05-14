package protocol

func encodeCommitOffsetRequest(value any) ([]byte, error) {
	return encodeWith(value, writeCommitOffsetRequest)
}

func writeCommitOffsetRequest(writer *binaryWriter, req CommitOffsetRequest) error {
	if err := writer.writeString(req.Consumer); err != nil {
		return err
	}
	if err := writer.writeString(req.Topic); err != nil {
		return err
	}
	writer.writeU32(req.Partition)
	writer.writeU64(req.NextOffset)
	return writer.writeString(req.Metadata)
}

func decodeCommitOffsetRequest(data []byte, target any) error {
	return decodeWith(data, target, readCommitOffsetRequest)
}

func readCommitOffsetRequest(reader *binaryReader) (CommitOffsetRequest, error) {
	consumer, err := reader.readString()
	if err != nil {
		return CommitOffsetRequest{}, err
	}
	topic, err := reader.readString()
	if err != nil {
		return CommitOffsetRequest{}, err
	}
	req := CommitOffsetRequest{Consumer: consumer, Topic: topic}
	if req.Partition, err = reader.readU32(); err != nil {
		return CommitOffsetRequest{}, err
	}
	if req.NextOffset, err = reader.readU64(); err != nil {
		return CommitOffsetRequest{}, err
	}
	if req.Metadata, err = reader.readString(); err != nil {
		return CommitOffsetRequest{}, err
	}
	return req, nil
}

func encodeCommitOffsetResponse(value any) ([]byte, error) {
	return encodeWith(value, writeCommitOffsetResponse)
}

func writeCommitOffsetResponse(writer *binaryWriter, resp CommitOffsetResponse) error {
	return writeCommitOffsetRequest(writer, CommitOffsetRequest(resp))
}

func decodeCommitOffsetResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (CommitOffsetResponse, error) {
		req, err := readCommitOffsetRequest(reader)
		return CommitOffsetResponse(req), err
	})
}

func encodeNackRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req NackRequest) error {
		if err := writer.writeString(req.Consumer); err != nil {
			return err
		}
		if err := writer.writeString(req.Topic); err != nil {
			return err
		}
		writer.writeU32(req.Partition)
		writer.writeU64(req.Offset)
		return writer.writeOptionalString(req.LastError)
	})
}

func decodeNackRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (NackRequest, error) {
		consumer, err := reader.readString()
		if err != nil {
			return NackRequest{}, err
		}
		topic, err := reader.readString()
		if err != nil {
			return NackRequest{}, err
		}
		req := NackRequest{Consumer: consumer, Topic: topic}
		if req.Partition, err = reader.readU32(); err != nil {
			return NackRequest{}, err
		}
		if req.Offset, err = reader.readU64(); err != nil {
			return NackRequest{}, err
		}
		if req.LastError, err = reader.readOptionalString(); err != nil {
			return NackRequest{}, err
		}
		return req, nil
	})
}

func encodeNackResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp NackResponse) error {
		if err := writer.writeString(resp.RetryTopic); err != nil {
			return err
		}
		writer.writeU32(resp.RetryPartition)
		writer.writeU64(resp.RetryOffset)
		writer.writeU64(resp.RetryNextOffset)
		writer.writeU32(resp.RetryCount)
		return nil
	})
}

func decodeNackResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (NackResponse, error) {
		topic, err := reader.readString()
		if err != nil {
			return NackResponse{}, err
		}
		resp := NackResponse{RetryTopic: topic}
		if resp.RetryPartition, err = reader.readU32(); err != nil {
			return NackResponse{}, err
		}
		if resp.RetryOffset, err = reader.readU64(); err != nil {
			return NackResponse{}, err
		}
		if resp.RetryNextOffset, err = reader.readU64(); err != nil {
			return NackResponse{}, err
		}
		if resp.RetryCount, err = reader.readU32(); err != nil {
			return NackResponse{}, err
		}
		return resp, nil
	})
}
