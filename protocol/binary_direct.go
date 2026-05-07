package protocol

func encodeSendDirectRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req SendDirectRequest) error {
		if err := writer.writeString(req.Sender); err != nil {
			return err
		}
		if err := writer.writeString(req.Recipient); err != nil {
			return err
		}
		if err := writer.writeOptionalString(req.ConversationID); err != nil {
			return err
		}
		return writer.writeBytes(req.Payload)
	})
}

func decodeSendDirectRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (SendDirectRequest, error) {
		sender, err := reader.readString()
		if err != nil {
			return SendDirectRequest{}, err
		}
		recipient, err := reader.readString()
		if err != nil {
			return SendDirectRequest{}, err
		}
		conversationID, err := reader.readOptionalString()
		if err != nil {
			return SendDirectRequest{}, err
		}
		payload, err := reader.readBytes()
		return SendDirectRequest{Sender: sender, Recipient: recipient, ConversationID: conversationID, Payload: payload}, err
	})
}

func encodeSendDirectResponse(value any) ([]byte, error) {
	return encodeWith(value, writeSendDirectResponse)
}

func writeSendDirectResponse(writer *binaryWriter, resp SendDirectResponse) error {
	if err := writer.writeString(resp.MessageID); err != nil {
		return err
	}
	if err := writer.writeString(resp.ConversationID); err != nil {
		return err
	}
	writer.writeU64(resp.Offset)
	writer.writeU64(resp.NextOffset)
	return nil
}

func decodeSendDirectResponse(data []byte, target any) error {
	return decodeWith(data, target, readSendDirectResponse)
}

func readSendDirectResponse(reader *binaryReader) (SendDirectResponse, error) {
	messageID, err := reader.readString()
	if err != nil {
		return SendDirectResponse{}, err
	}
	conversationID, err := reader.readString()
	if err != nil {
		return SendDirectResponse{}, err
	}
	resp := SendDirectResponse{MessageID: messageID, ConversationID: conversationID}
	if resp.Offset, err = reader.readU64(); err != nil {
		return SendDirectResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return SendDirectResponse{}, err
	}
	return resp, nil
}

func encodeFetchInboxRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req FetchInboxRequest) error {
		maxRecords, err := checkedUint32(req.MaxRecords, "max_records")
		if err != nil {
			return err
		}
		if err := writer.writeString(req.Recipient); err != nil {
			return err
		}
		writer.writeU32(maxRecords)
		return nil
	})
}

func decodeFetchInboxRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (FetchInboxRequest, error) {
		recipient, err := reader.readString()
		if err != nil {
			return FetchInboxRequest{}, err
		}
		maxRecords, err := reader.readU32()
		if err != nil {
			return FetchInboxRequest{}, err
		}
		out, err := intFromUint32(maxRecords)
		return FetchInboxRequest{Recipient: recipient, MaxRecords: out}, err
	})
}

func encodeFetchInboxResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp FetchInboxResponse) error {
		if err := writer.writeString(resp.Recipient); err != nil {
			return err
		}
		if err := writeDirectMessageRecords(writer, resp.Records); err != nil {
			return err
		}
		writer.writeU64(resp.NextOffset)
		writer.writeOptionalU64(resp.HighWatermark)
		return nil
	})
}

func decodeFetchInboxResponse(data []byte, target any) error {
	return decodeWith(data, target, readFetchInboxResponse)
}

func readFetchInboxResponse(reader *binaryReader) (FetchInboxResponse, error) {
	recipient, err := reader.readString()
	if err != nil {
		return FetchInboxResponse{}, err
	}
	resp := FetchInboxResponse{Recipient: recipient}
	if resp.Records, err = readDirectMessageRecords(reader); err != nil {
		return FetchInboxResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return FetchInboxResponse{}, err
	}
	if resp.HighWatermark, err = reader.readOptionalU64(); err != nil {
		return FetchInboxResponse{}, err
	}
	return resp, nil
}

func writeDirectMessageRecords(writer *binaryWriter, records []DirectMessageRecord) error {
	count, err := checkedUint32(len(records), "direct_records")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, record := range records {
		if err := writeDirectMessageRecord(writer, record); err != nil {
			return err
		}
	}
	return nil
}

func readDirectMessageRecords(reader *binaryReader) ([]DirectMessageRecord, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	records := newDecodedList[DirectMessageRecord](size)
	for range size {
		record, err := readDirectMessageRecord(reader)
		if err != nil {
			return nil, err
		}
		records.Add(record)
	}
	return records.Values(), nil
}

func writeDirectMessageRecord(writer *binaryWriter, record DirectMessageRecord) error {
	writer.writeU64(record.Offset)
	if err := writer.writeString(record.MessageID); err != nil {
		return err
	}
	if err := writer.writeString(record.ConversationID); err != nil {
		return err
	}
	if err := writer.writeString(record.Sender); err != nil {
		return err
	}
	if err := writer.writeString(record.Recipient); err != nil {
		return err
	}
	writer.writeU64(record.TimestampMS)
	return writer.writeBytes(record.Payload)
}

func readDirectMessageRecord(reader *binaryReader) (DirectMessageRecord, error) {
	var record DirectMessageRecord
	var err error
	if record.Offset, err = reader.readU64(); err != nil {
		return DirectMessageRecord{}, err
	}
	return readDirectMessageRecordStrings(reader, record)
}

func readDirectMessageRecordStrings(reader *binaryReader, record DirectMessageRecord) (DirectMessageRecord, error) {
	var err error
	if record.MessageID, err = reader.readString(); err != nil {
		return DirectMessageRecord{}, err
	}
	if record.ConversationID, err = reader.readString(); err != nil {
		return DirectMessageRecord{}, err
	}
	if record.Sender, err = reader.readString(); err != nil {
		return DirectMessageRecord{}, err
	}
	if record.Recipient, err = reader.readString(); err != nil {
		return DirectMessageRecord{}, err
	}
	if record.TimestampMS, err = reader.readU64(); err != nil {
		return DirectMessageRecord{}, err
	}
	if record.Payload, err = reader.readBytes(); err != nil {
		return DirectMessageRecord{}, err
	}
	return record, nil
}
