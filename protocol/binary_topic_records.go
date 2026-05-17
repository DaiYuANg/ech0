package protocol

func encodeProduceRequest(value any) ([]byte, error) {
	return encodeWith(value, writeProduceRequest)
}

func writeProduceRequest(writer *binaryWriter, req ProduceRequest) error {
	if err := writer.writeString(req.Topic); err != nil {
		return err
	}
	writer.writeOptionalU32(req.Partition)
	writePartitioning(writer, req.Partitioning)
	writeProduceIdempotency(writer, req.Idempotency)
	if err := writer.writeBytes(req.Key); err != nil {
		return err
	}
	if err := writeHeaders(writer, req.Headers); err != nil {
		return err
	}
	writer.writeBool(req.Tombstone)
	writer.writeOptionalU64(req.ExpiresAtMS)
	return writer.writeBytes(req.Payload)
}

func decodeProduceRequest(data []byte, target any) error {
	return decodeWith(data, target, readProduceRequest)
}

func readProduceRequest(reader *binaryReader) (ProduceRequest, error) {
	topic, err := reader.readString()
	if err != nil {
		return ProduceRequest{}, err
	}
	req := ProduceRequest{Topic: topic}
	if req.Partition, err = reader.readOptionalU32(); err != nil {
		return ProduceRequest{}, err
	}
	if req.Partitioning, err = readPartitioning(reader); err != nil {
		return ProduceRequest{}, err
	}
	if req.Idempotency, err = readProduceIdempotency(reader); err != nil {
		return ProduceRequest{}, err
	}
	if req.Key, err = reader.readBytes(); err != nil {
		return ProduceRequest{}, err
	}
	if req.Headers, err = readHeaders(reader); err != nil {
		return ProduceRequest{}, err
	}
	if req.Tombstone, err = reader.readBool(); err != nil {
		return ProduceRequest{}, err
	}
	if req.ExpiresAtMS, err = reader.readOptionalU64(); err != nil {
		return ProduceRequest{}, err
	}
	if req.Payload, err = reader.readBytes(); err != nil {
		return ProduceRequest{}, err
	}
	return req, nil
}

func encodeProduceResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp ProduceResponse) error {
		writer.writeU32(resp.Partition)
		writer.writeU64(resp.Offset)
		writer.writeU64(resp.NextOffset)
		return nil
	})
}

func decodeProduceResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ProduceResponse, error) {
		var resp ProduceResponse
		var err error
		if resp.Partition, err = reader.readU32(); err != nil {
			return ProduceResponse{}, err
		}
		if resp.Offset, err = reader.readU64(); err != nil {
			return ProduceResponse{}, err
		}
		if resp.NextOffset, err = reader.readU64(); err != nil {
			return ProduceResponse{}, err
		}
		return resp, nil
	})
}

func encodeProduceBatchRequest(value any) ([]byte, error) {
	return encodeWith(value, writeProduceBatchRequest)
}

func writeProduceBatchRequest(writer *binaryWriter, req ProduceBatchRequest) error {
	if err := writer.writeString(req.Topic); err != nil {
		return err
	}
	writer.writeOptionalU32(req.Partition)
	writePartitioning(writer, req.Partitioning)
	writeProduceIdempotency(writer, req.Idempotency)
	if err := writePayloads(writer, req.Payloads); err != nil {
		return err
	}
	return writeBatchRecords(writer, req.Records)
}

func decodeProduceBatchRequest(data []byte, target any) error {
	return decodeWith(data, target, readProduceBatchRequest)
}

func readProduceBatchRequest(reader *binaryReader) (ProduceBatchRequest, error) {
	topic, err := reader.readString()
	if err != nil {
		return ProduceBatchRequest{}, err
	}
	req := ProduceBatchRequest{Topic: topic}
	if req.Partition, err = reader.readOptionalU32(); err != nil {
		return ProduceBatchRequest{}, err
	}
	if req.Partitioning, err = readPartitioning(reader); err != nil {
		return ProduceBatchRequest{}, err
	}
	if req.Idempotency, err = readProduceIdempotency(reader); err != nil {
		return ProduceBatchRequest{}, err
	}
	if req.Payloads, err = readPayloads(reader); err != nil {
		return ProduceBatchRequest{}, err
	}
	if req.Records, err = readBatchRecords(reader); err != nil {
		return ProduceBatchRequest{}, err
	}
	return req, nil
}

func writePayloads(writer *binaryWriter, payloads [][]byte) error {
	count, err := checkedUint32(len(payloads), "payloads")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, payload := range payloads {
		if err := writer.writeBytes(payload); err != nil {
			return err
		}
	}
	return nil
}

func readPayloads(reader *binaryReader) ([][]byte, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	out := newDecodedList[[]byte](size)
	for range size {
		payload, err := reader.readBytes()
		if err != nil {
			return nil, err
		}
		out.Add(payload)
	}
	return out.Values(), nil
}

func writeBatchRecords(writer *binaryWriter, records []ProduceBatchRecord) error {
	count, err := checkedUint32(len(records), "records")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, record := range records {
		if err := writeBatchRecord(writer, record); err != nil {
			return err
		}
	}
	return nil
}

func writeBatchRecord(writer *binaryWriter, record ProduceBatchRecord) error {
	if err := writer.writeBytes(record.Key); err != nil {
		return err
	}
	if err := writeHeaders(writer, record.Headers); err != nil {
		return err
	}
	writer.writeBool(record.Tombstone)
	writer.writeOptionalU64(record.ExpiresAtMS)
	return writer.writeBytes(record.Payload)
}

func readBatchRecords(reader *binaryReader) ([]ProduceBatchRecord, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	out := newDecodedList[ProduceBatchRecord](size)
	for range size {
		record, err := readBatchRecord(reader)
		if err != nil {
			return nil, err
		}
		out.Add(record)
	}
	return out.Values(), nil
}

func readBatchRecord(reader *binaryReader) (ProduceBatchRecord, error) {
	var record ProduceBatchRecord
	var err error
	if record.Key, err = reader.readBytes(); err != nil {
		return ProduceBatchRecord{}, err
	}
	if record.Headers, err = readHeaders(reader); err != nil {
		return ProduceBatchRecord{}, err
	}
	if record.Tombstone, err = reader.readBool(); err != nil {
		return ProduceBatchRecord{}, err
	}
	if record.ExpiresAtMS, err = reader.readOptionalU64(); err != nil {
		return ProduceBatchRecord{}, err
	}
	if record.Payload, err = reader.readBytes(); err != nil {
		return ProduceBatchRecord{}, err
	}
	return record, nil
}

func encodeProduceBatchResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp ProduceBatchResponse) error {
		appended, err := checkedUint32(resp.Appended, "appended")
		if err != nil {
			return err
		}
		writer.writeU32(resp.Partition)
		writer.writeU64(resp.BaseOffset)
		writer.writeU64(resp.LastOffset)
		writer.writeU64(resp.NextOffset)
		writer.writeU32(appended)
		return nil
	})
}

func decodeProduceBatchResponse(data []byte, target any) error {
	return decodeWith(data, target, readProduceBatchResponse)
}

func readProduceBatchResponse(reader *binaryReader) (ProduceBatchResponse, error) {
	var resp ProduceBatchResponse
	var err error
	if resp.Partition, err = reader.readU32(); err != nil {
		return ProduceBatchResponse{}, err
	}
	if resp.BaseOffset, err = reader.readU64(); err != nil {
		return ProduceBatchResponse{}, err
	}
	if resp.LastOffset, err = reader.readU64(); err != nil {
		return ProduceBatchResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return ProduceBatchResponse{}, err
	}
	appended, err := reader.readU32()
	if err != nil {
		return ProduceBatchResponse{}, err
	}
	resp.Appended, err = intFromUint32(appended)
	return resp, err
}
