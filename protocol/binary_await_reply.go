package protocol

func encodeAwaitReplyRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req AwaitReplyRequest) error {
		if err := writer.writeString(req.ReplyTo); err != nil {
			return err
		}
		if err := writer.writeString(req.CorrelationID); err != nil {
			return err
		}
		writer.writeU64(req.ExpiresAtMS)
		writer.writeOptionalU64(req.TimeoutMS)
		writer.writeOptionalU64(req.PollIntervalMS)
		return nil
	})
}

func decodeAwaitReplyRequest(data []byte, target any) error {
	return decodeWith(data, target, readAwaitReplyRequest)
}

func encodeAwaitReplyResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp AwaitReplyResponse) error {
		return writeReplyRecord(writer, resp.Reply)
	})
}

func decodeAwaitReplyResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (AwaitReplyResponse, error) {
		reply, err := readReplyRecord(reader)
		return AwaitReplyResponse{Reply: reply}, err
	})
}

func encodeAwaitRepliesRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req AwaitRepliesRequest) error {
		maxReplies, err := checkedUint32(req.MaxReplies, "max_replies")
		if err != nil {
			return err
		}
		if err := writer.writeString(req.ReplyTo); err != nil {
			return err
		}
		if err := writer.writeString(req.CorrelationID); err != nil {
			return err
		}
		writer.writeU64(req.ExpiresAtMS)
		writer.writeOptionalU64(req.TimeoutMS)
		writer.writeOptionalU64(req.PollIntervalMS)
		writer.writeU32(maxReplies)
		return nil
	})
}

func decodeAwaitRepliesRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (AwaitRepliesRequest, error) {
		await, err := readAwaitReplyRequest(reader)
		if err != nil {
			return AwaitRepliesRequest{}, err
		}
		maxReplies, err := reader.readU32()
		if err != nil {
			return AwaitRepliesRequest{}, err
		}
		out, err := intFromUint32(maxReplies)
		if err != nil {
			return AwaitRepliesRequest{}, err
		}
		return AwaitRepliesRequest{
			ReplyTo:        await.ReplyTo,
			CorrelationID:  await.CorrelationID,
			ExpiresAtMS:    await.ExpiresAtMS,
			TimeoutMS:      await.TimeoutMS,
			PollIntervalMS: await.PollIntervalMS,
			MaxReplies:     out,
		}, nil
	})
}

func encodeAwaitRepliesResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp AwaitRepliesResponse) error {
		return writeReplyRecords(writer, resp.Replies)
	})
}

func decodeAwaitRepliesResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (AwaitRepliesResponse, error) {
		replies, err := readReplyRecords(reader)
		return AwaitRepliesResponse{Replies: replies}, err
	})
}

func readAwaitReplyRequest(reader *binaryReader) (AwaitReplyRequest, error) {
	replyTo, err := reader.readString()
	if err != nil {
		return AwaitReplyRequest{}, err
	}
	correlationID, err := reader.readString()
	if err != nil {
		return AwaitReplyRequest{}, err
	}
	req := AwaitReplyRequest{ReplyTo: replyTo, CorrelationID: correlationID}
	if req.ExpiresAtMS, err = reader.readU64(); err != nil {
		return AwaitReplyRequest{}, err
	}
	if req.TimeoutMS, err = reader.readOptionalU64(); err != nil {
		return AwaitReplyRequest{}, err
	}
	if req.PollIntervalMS, err = reader.readOptionalU64(); err != nil {
		return AwaitReplyRequest{}, err
	}
	return req, nil
}

func writeReplyRecords(writer *binaryWriter, records []ReplyRecord) error {
	count, err := checkedUint32(len(records), "reply_records")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for index := range records {
		if err := writeReplyRecord(writer, records[index]); err != nil {
			return err
		}
	}
	return nil
}

func readReplyRecords(reader *binaryReader) ([]ReplyRecord, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	records := newDecodedList[ReplyRecord](size)
	for range size {
		record, err := readReplyRecord(reader)
		if err != nil {
			return nil, err
		}
		records.Add(record)
	}
	return records.Values(), nil
}

func writeReplyRecord(writer *binaryWriter, record ReplyRecord) error {
	writer.writeU64(record.Offset)
	if err := writer.writeString(record.MessageID); err != nil {
		return err
	}
	writer.writeU64(record.TimestampMS)
	if err := writer.writeString(record.Subject); err != nil {
		return err
	}
	if err := writer.writeString(record.CorrelationID); err != nil {
		return err
	}
	if err := writer.writeString(record.ResponderID); err != nil {
		return err
	}
	if err := writer.writeOptionalString(record.Error); err != nil {
		return err
	}
	return writer.writeBytes(record.Payload)
}

func readReplyRecord(reader *binaryReader) (ReplyRecord, error) {
	var record ReplyRecord
	var err error
	if record.Offset, err = reader.readU64(); err != nil {
		return ReplyRecord{}, err
	}
	if record.MessageID, err = reader.readString(); err != nil {
		return ReplyRecord{}, err
	}
	if record.TimestampMS, err = reader.readU64(); err != nil {
		return ReplyRecord{}, err
	}
	return readReplyRecordTail(reader, record)
}

func readReplyRecordTail(reader *binaryReader, record ReplyRecord) (ReplyRecord, error) {
	var err error
	if record.Subject, err = reader.readString(); err != nil {
		return ReplyRecord{}, err
	}
	if record.CorrelationID, err = reader.readString(); err != nil {
		return ReplyRecord{}, err
	}
	if record.ResponderID, err = reader.readString(); err != nil {
		return ReplyRecord{}, err
	}
	if record.Error, err = reader.readOptionalString(); err != nil {
		return ReplyRecord{}, err
	}
	if record.Payload, err = reader.readBytes(); err != nil {
		return ReplyRecord{}, err
	}
	return record, nil
}
