package protocol

func encodeCreateTopicRequest(value any) ([]byte, error) {
	return encodeWith(value, writeCreateTopicRequest)
}

func writeCreateTopicRequest(writer *binaryWriter, req CreateTopicRequest) error {
	if err := writer.writeString(req.Topic); err != nil {
		return err
	}
	writer.writeU32(req.Partitions)
	writer.writeOptionalU64(req.RetentionMaxBytes)
	writeCleanupPolicy(writer, req.CleanupPolicy)
	writer.writeOptionalU32(req.MaxMessageBytes)
	writer.writeOptionalU32(req.MaxBatchBytes)
	writer.writeOptionalU64(req.RetentionMS)
	writeOptionalRetryPolicy(writer, req.RetryPolicy)
	if err := writer.writeOptionalString(req.DeadLetterTopic); err != nil {
		return err
	}
	writer.writeOptionalBool(req.DelayEnabled)
	writer.writeOptionalU64(req.MessageTTLMS)
	writeMessageExpiryAction(writer, req.MessageExpiryAction)
	writer.writeOptionalBool(req.CompactionEnabled)
	writer.writeOptionalU64(req.CompactionTombstoneRetentionMS)
	writeTopicOrderingPolicy(writer, req.OrderingPolicy)
	return nil
}

func decodeCreateTopicRequest(data []byte, target any) error {
	return decodeWith(data, target, readCreateTopicRequest)
}

func readCreateTopicRequest(reader *binaryReader) (CreateTopicRequest, error) {
	topic, err := reader.readString()
	if err != nil {
		return CreateTopicRequest{}, err
	}
	req := CreateTopicRequest{Topic: topic}
	if req.Partitions, err = reader.readU32(); err != nil {
		return CreateTopicRequest{}, err
	}
	return readCreateTopicOptions(reader, req)
}

func readCreateTopicOptions(reader *binaryReader, req CreateTopicRequest) (CreateTopicRequest, error) {
	var err error
	if req.RetentionMaxBytes, err = reader.readOptionalU64(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.CleanupPolicy, err = readCleanupPolicy(reader); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.MaxMessageBytes, err = reader.readOptionalU32(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.MaxBatchBytes, err = reader.readOptionalU32(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.RetentionMS, err = reader.readOptionalU64(); err != nil {
		return CreateTopicRequest{}, err
	}
	return readCreateTopicRemainingOptions(reader, req)
}

func readCreateTopicRemainingOptions(reader *binaryReader, req CreateTopicRequest) (CreateTopicRequest, error) {
	var err error
	if req.RetryPolicy, err = readOptionalRetryPolicy(reader); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.DeadLetterTopic, err = reader.readOptionalString(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.DelayEnabled, err = reader.readOptionalBool(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.MessageTTLMS, err = reader.readOptionalU64(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.MessageExpiryAction, err = readMessageExpiryAction(reader); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.CompactionEnabled, err = reader.readOptionalBool(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.CompactionTombstoneRetentionMS, err = reader.readOptionalU64(); err != nil {
		return CreateTopicRequest{}, err
	}
	if req.OrderingPolicy, err = readTopicOrderingPolicy(reader); err != nil {
		return CreateTopicRequest{}, err
	}
	return req, nil
}

func writeOptionalRetryPolicy(writer *binaryWriter, policy *TopicRetryPolicy) {
	if policy == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeU32(policy.MaxAttempts)
	writer.writeU64(policy.BackoffInitialMS)
	writer.writeU64(policy.BackoffMaxMS)
	writer.writeF64(policy.BackoffJitterFactor)
}

func readOptionalRetryPolicy(reader *binaryReader) (*TopicRetryPolicy, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return nil, err
	}
	policy := TopicRetryPolicy{}
	if policy.MaxAttempts, err = reader.readU32(); err != nil {
		return nil, err
	}
	if policy.BackoffInitialMS, err = reader.readU64(); err != nil {
		return nil, err
	}
	if policy.BackoffMaxMS, err = reader.readU64(); err != nil {
		return nil, err
	}
	if policy.BackoffJitterFactor, err = reader.readF64(); err != nil {
		return nil, err
	}
	return &policy, nil
}

func encodeCreateTopicResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp CreateTopicResponse) error {
		if err := writer.writeString(resp.Topic); err != nil {
			return err
		}
		writer.writeU32(resp.Partitions)
		return nil
	})
}

func decodeCreateTopicResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (CreateTopicResponse, error) {
		topic, err := reader.readString()
		if err != nil {
			return CreateTopicResponse{}, err
		}
		partitions, err := reader.readU32()
		return CreateTopicResponse{Topic: topic, Partitions: partitions}, err
	})
}

func encodeListTopicsRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req ListTopicsRequest) error {
		return writer.writeString(req.Pattern)
	})
}

func decodeListTopicsRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ListTopicsRequest, error) {
		pattern, err := reader.readString()
		return ListTopicsRequest{Pattern: pattern}, err
	})
}

func encodeListTopicsResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp ListTopicsResponse) error {
		count, err := checkedUint32(len(resp.Topics), "topics")
		if err != nil {
			return err
		}
		writer.writeU32(count)
		for _, topic := range resp.Topics {
			if err := writer.writeString(topic.Topic); err != nil {
				return err
			}
			writer.writeU32(topic.Partitions)
		}
		return nil
	})
}

func decodeListTopicsResponse(data []byte, target any) error {
	return decodeWith(data, target, readListTopicsResponse)
}

func readListTopicsResponse(reader *binaryReader) (ListTopicsResponse, error) {
	count, err := reader.readU32()
	if err != nil {
		return ListTopicsResponse{}, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return ListTopicsResponse{}, err
	}
	topics := newDecodedList[TopicMetadata](size)
	for range size {
		name, err := reader.readString()
		if err != nil {
			return ListTopicsResponse{}, err
		}
		partitions, err := reader.readU32()
		if err != nil {
			return ListTopicsResponse{}, err
		}
		topics.Add(TopicMetadata{Topic: name, Partitions: partitions})
	}
	return ListTopicsResponse{Topics: topics.Values()}, nil
}

func encodeErrorResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp ErrorResponse) error {
		if err := writer.writeString(resp.Code); err != nil {
			return err
		}
		return writer.writeString(resp.Message)
	})
}

func decodeErrorResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ErrorResponse, error) {
		code, err := reader.readString()
		if err != nil {
			return ErrorResponse{}, err
		}
		message, err := reader.readString()
		return ErrorResponse{Code: code, Message: message}, err
	})
}
