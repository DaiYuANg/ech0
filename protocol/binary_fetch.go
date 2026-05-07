package protocol

func encodeFetchRequest(value any) ([]byte, error) {
	return encodeWith(value, writeFetchRequest)
}

func writeFetchRequest(writer *binaryWriter, req FetchRequest) error {
	maxRecords, err := checkedUint32(req.MaxRecords, "max_records")
	if err != nil {
		return err
	}
	if err := writer.writeString(req.Consumer); err != nil {
		return err
	}
	if err := writer.writeString(req.Topic); err != nil {
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
}

func decodeFetchRequest(data []byte, target any) error {
	return decodeWith(data, target, readFetchRequest)
}

func readFetchRequest(reader *binaryReader) (FetchRequest, error) {
	consumer, err := reader.readString()
	if err != nil {
		return FetchRequest{}, err
	}
	topic, err := reader.readString()
	if err != nil {
		return FetchRequest{}, err
	}
	req := FetchRequest{Consumer: consumer, Topic: topic}
	if req.Partition, err = reader.readU32(); err != nil {
		return FetchRequest{}, err
	}
	if req.Offset, err = reader.readOptionalU64(); err != nil {
		return FetchRequest{}, err
	}
	maxRecords, err := reader.readU32()
	if err != nil {
		return FetchRequest{}, err
	}
	if req.MaxRecords, err = intFromUint32(maxRecords); err != nil {
		return FetchRequest{}, err
	}
	if req.MinRecords, err = reader.readOptionalInt(); err != nil {
		return FetchRequest{}, err
	}
	if req.MaxWaitMS, err = reader.readOptionalU64(); err != nil {
		return FetchRequest{}, err
	}
	return req, nil
}

func encodeFetchResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp FetchResponse) error {
		if err := writer.writeString(resp.Topic); err != nil {
			return err
		}
		writer.writeU32(resp.Partition)
		if err := writeFetchRecords(writer, resp.Records); err != nil {
			return err
		}
		writer.writeU64(resp.NextOffset)
		writer.writeOptionalU64(resp.HighWatermark)
		return nil
	})
}

func decodeFetchResponse(data []byte, target any) error {
	return decodeWith(data, target, readFetchResponse)
}

func readFetchResponse(reader *binaryReader) (FetchResponse, error) {
	topic, err := reader.readString()
	if err != nil {
		return FetchResponse{}, err
	}
	resp := FetchResponse{Topic: topic}
	if resp.Partition, err = reader.readU32(); err != nil {
		return FetchResponse{}, err
	}
	if resp.Records, err = readFetchRecords(reader); err != nil {
		return FetchResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return FetchResponse{}, err
	}
	if resp.HighWatermark, err = reader.readOptionalU64(); err != nil {
		return FetchResponse{}, err
	}
	return resp, nil
}

func writeFetchRecords(writer *binaryWriter, records []FetchRecord) error {
	count, err := checkedUint32(len(records), "fetch_records")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, record := range records {
		if err := writeFetchRecord(writer, record); err != nil {
			return err
		}
	}
	return nil
}

func readFetchRecords(reader *binaryReader) ([]FetchRecord, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	records := newDecodedList[FetchRecord](size)
	for range size {
		record, err := readFetchRecord(reader)
		if err != nil {
			return nil, err
		}
		records.Add(record)
	}
	return records.Values(), nil
}

func writeFetchRecord(writer *binaryWriter, record FetchRecord) error {
	writer.writeU64(record.Offset)
	writer.writeU64(record.TimestampMS)
	if err := writer.writeBytes(record.Key); err != nil {
		return err
	}
	if err := writeHeaders(writer, record.Headers); err != nil {
		return err
	}
	writer.writeBool(record.Tombstone)
	return writer.writeBytes(record.Payload)
}

func readFetchRecord(reader *binaryReader) (FetchRecord, error) {
	var record FetchRecord
	var err error
	if record.Offset, err = reader.readU64(); err != nil {
		return FetchRecord{}, err
	}
	if record.TimestampMS, err = reader.readU64(); err != nil {
		return FetchRecord{}, err
	}
	if record.Key, err = reader.readBytes(); err != nil {
		return FetchRecord{}, err
	}
	if record.Headers, err = readHeaders(reader); err != nil {
		return FetchRecord{}, err
	}
	if record.Tombstone, err = reader.readBool(); err != nil {
		return FetchRecord{}, err
	}
	if record.Payload, err = reader.readBytes(); err != nil {
		return FetchRecord{}, err
	}
	return record, nil
}
