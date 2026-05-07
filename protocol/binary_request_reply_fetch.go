package protocol

func encodeFetchRequestsRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req FetchRequestsRequest) error {
		maxRecords, err := checkedUint32(req.MaxRecords, "max_records")
		if err != nil {
			return err
		}
		if err := writer.writeString(req.Consumer); err != nil {
			return err
		}
		if err := writer.writeString(req.Subject); err != nil {
			return err
		}
		writer.writeU32(req.Partition)
		writer.writeOptionalU64(req.Offset)
		writer.writeU32(maxRecords)
		if err := writer.writeOptionalInt(req.MinRecords); err != nil {
			return err
		}
		writer.writeOptionalU64(req.MaxWaitMS)
		return nil
	})
}

func decodeFetchRequestsRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (FetchRequestsRequest, error) {
		fetch, err := readFetchRequest(reader)
		return FetchRequestsRequest{
			Consumer: fetch.Consumer, Subject: fetch.Topic, Partition: fetch.Partition, Offset: fetch.Offset,
			MaxRecords: fetch.MaxRecords, MinRecords: fetch.MinRecords, MaxWaitMS: fetch.MaxWaitMS,
		}, err
	})
}

func encodeFetchRequestsResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp FetchRequestsResponse) error {
		if err := writer.writeString(resp.Subject); err != nil {
			return err
		}
		writer.writeU32(resp.Partition)
		if err := writeRequestRecords(writer, resp.Requests); err != nil {
			return err
		}
		writer.writeU64(resp.NextOffset)
		writer.writeOptionalU64(resp.HighWatermark)
		return nil
	})
}

func decodeFetchRequestsResponse(data []byte, target any) error {
	return decodeWith(data, target, readFetchRequestsResponse)
}

func readFetchRequestsResponse(reader *binaryReader) (FetchRequestsResponse, error) {
	subject, err := reader.readString()
	if err != nil {
		return FetchRequestsResponse{}, err
	}
	resp := FetchRequestsResponse{Subject: subject}
	if resp.Partition, err = reader.readU32(); err != nil {
		return FetchRequestsResponse{}, err
	}
	if resp.Requests, err = readRequestRecords(reader); err != nil {
		return FetchRequestsResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return FetchRequestsResponse{}, err
	}
	if resp.HighWatermark, err = reader.readOptionalU64(); err != nil {
		return FetchRequestsResponse{}, err
	}
	return resp, nil
}

func writeRequestRecords(writer *binaryWriter, records []RequestRecord) error {
	count, err := checkedUint32(len(records), "request_records")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for i := range records {
		if err := writeRequestRecord(writer, records[i]); err != nil {
			return err
		}
	}
	return nil
}

func readRequestRecords(reader *binaryReader) ([]RequestRecord, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	records := newDecodedList[RequestRecord](size)
	for range size {
		record, err := readRequestRecord(reader)
		if err != nil {
			return nil, err
		}
		records.Add(record)
	}
	return records.Values(), nil
}

func writeRequestRecord(writer *binaryWriter, record RequestRecord) error {
	writer.writeU64(record.Offset)
	writer.writeU64(record.TimestampMS)
	if err := writer.writeString(record.Subject); err != nil {
		return err
	}
	if err := writer.writeString(record.ReplyTo); err != nil {
		return err
	}
	if err := writer.writeString(record.CorrelationID); err != nil {
		return err
	}
	if err := writer.writeString(record.SenderID); err != nil {
		return err
	}
	writer.writeU64(record.ExpiresAtMS)
	if err := writeHeaders(writer, record.Headers); err != nil {
		return err
	}
	return writer.writeBytes(record.Payload)
}

func readRequestRecord(reader *binaryReader) (RequestRecord, error) {
	var record RequestRecord
	var err error
	if record.Offset, err = reader.readU64(); err != nil {
		return RequestRecord{}, err
	}
	if record.TimestampMS, err = reader.readU64(); err != nil {
		return RequestRecord{}, err
	}
	return readRequestRecordTail(reader, record)
}

func readRequestRecordTail(reader *binaryReader, record RequestRecord) (RequestRecord, error) {
	var err error
	if record.Subject, err = reader.readString(); err != nil {
		return RequestRecord{}, err
	}
	if record.ReplyTo, err = reader.readString(); err != nil {
		return RequestRecord{}, err
	}
	if record.CorrelationID, err = reader.readString(); err != nil {
		return RequestRecord{}, err
	}
	if record.SenderID, err = reader.readString(); err != nil {
		return RequestRecord{}, err
	}
	if record.ExpiresAtMS, err = reader.readU64(); err != nil {
		return RequestRecord{}, err
	}
	if record.Headers, err = readHeaders(reader); err != nil {
		return RequestRecord{}, err
	}
	if record.Payload, err = reader.readBytes(); err != nil {
		return RequestRecord{}, err
	}
	return record, nil
}
