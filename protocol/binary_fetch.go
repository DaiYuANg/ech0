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
	writeFetchIsolation(writer, req.Isolation)
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
	if req.Isolation, err = readFetchIsolation(reader); err != nil {
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
		writeFetchWatermarks(writer, resp.HighWatermark, resp.LowWatermark, resp.LogStartOffset)
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
	if err := readFetchWatermarks(reader, &resp.HighWatermark, &resp.LowWatermark, &resp.LogStartOffset); err != nil {
		return FetchResponse{}, err
	}
	return resp, nil
}

func writeFetchWatermarks(writer *binaryWriter, high, low *uint64, logStart uint64) {
	writer.writeOptionalU64(high)
	writer.writeOptionalU64(low)
	writer.writeU64(logStart)
}

func readFetchWatermarks(reader *binaryReader, high, low **uint64, logStart *uint64) error {
	var err error
	if *high, err = reader.readOptionalU64(); err != nil {
		return err
	}
	if *low, err = reader.readOptionalU64(); err != nil {
		return err
	}
	*logStart, err = reader.readU64()
	return err
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
	writeOptionalTransactionRecordMetadata(writer, record.Transaction)
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
	if record.Transaction, err = readOptionalTransactionRecordMetadata(reader); err != nil {
		return FetchRecord{}, err
	}
	if record.Payload, err = reader.readBytes(); err != nil {
		return FetchRecord{}, err
	}
	return record, nil
}

func writeOptionalTransactionRecordMetadata(writer *binaryWriter, tx *TransactionRecordMetadata) {
	if tx == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeU64(tx.TxID)
	writer.writeU64(tx.ProducerID)
	writer.writeU64(tx.ProducerEpoch)
	writer.writeU64(tx.Sequence)
	writeTransactionControlType(writer, tx.ControlType)
}

func readOptionalTransactionRecordMetadata(reader *binaryReader) (*TransactionRecordMetadata, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return nil, err
	}
	tx := TransactionRecordMetadata{}
	if tx.TxID, err = reader.readU64(); err != nil {
		return nil, err
	}
	if tx.ProducerID, err = reader.readU64(); err != nil {
		return nil, err
	}
	if tx.ProducerEpoch, err = reader.readU64(); err != nil {
		return nil, err
	}
	if tx.Sequence, err = reader.readU64(); err != nil {
		return nil, err
	}
	if tx.ControlType, err = readTransactionControlType(reader); err != nil {
		return nil, err
	}
	return &tx, nil
}
