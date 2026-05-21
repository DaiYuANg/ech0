package protocol

func encodeProduceFanoutRequest(value any) ([]byte, error) {
	return encodeWith(value, writeProduceFanoutRequest)
}

func writeProduceFanoutRequest(writer *binaryWriter, req ProduceFanoutRequest) error {
	if err := writer.writeString(req.Topic); err != nil {
		return err
	}
	if err := writer.writeString(req.RoutingKey); err != nil {
		return err
	}
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

func decodeProduceFanoutRequest(data []byte, target any) error {
	return decodeWith(data, target, readProduceFanoutRequest)
}

func readProduceFanoutRequest(reader *binaryReader) (ProduceFanoutRequest, error) {
	var req ProduceFanoutRequest
	var err error
	if req.Topic, err = reader.readString(); err != nil {
		return ProduceFanoutRequest{}, err
	}
	if req.RoutingKey, err = reader.readString(); err != nil {
		return ProduceFanoutRequest{}, err
	}
	if req.Key, err = reader.readBytes(); err != nil {
		return ProduceFanoutRequest{}, err
	}
	if req.Headers, err = readHeaders(reader); err != nil {
		return ProduceFanoutRequest{}, err
	}
	if req.Tombstone, err = reader.readBool(); err != nil {
		return ProduceFanoutRequest{}, err
	}
	if req.ExpiresAtMS, err = reader.readOptionalU64(); err != nil {
		return ProduceFanoutRequest{}, err
	}
	if req.Payload, err = reader.readBytes(); err != nil {
		return ProduceFanoutRequest{}, err
	}
	return req, nil
}

func encodeProduceFanoutResponse(value any) ([]byte, error) {
	return encodeWith(value, writeProduceFanoutResponse)
}

func writeProduceFanoutResponse(writer *binaryWriter, resp ProduceFanoutResponse) error {
	count, err := checkedUint32(len(resp.Records), "fanout records")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for index := range resp.Records {
		record := resp.Records[index]
		writer.writeU32(record.Partition)
		writer.writeU64(record.Offset)
		writer.writeU64(record.NextOffset)
	}
	return nil
}

func decodeProduceFanoutResponse(data []byte, target any) error {
	return decodeWith(data, target, readProduceFanoutResponse)
}

func readProduceFanoutResponse(reader *binaryReader) (ProduceFanoutResponse, error) {
	count, err := reader.readU32()
	if err != nil {
		return ProduceFanoutResponse{}, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return ProduceFanoutResponse{}, err
	}
	records := newDecodedList[ProduceFanoutRecordResponse](size)
	for range size {
		record, err := readProduceFanoutRecordResponse(reader)
		if err != nil {
			return ProduceFanoutResponse{}, err
		}
		records.Add(record)
	}
	return ProduceFanoutResponse{Records: records.Values()}, nil
}

func readProduceFanoutRecordResponse(reader *binaryReader) (ProduceFanoutRecordResponse, error) {
	var record ProduceFanoutRecordResponse
	var err error
	if record.Partition, err = reader.readU32(); err != nil {
		return ProduceFanoutRecordResponse{}, err
	}
	if record.Offset, err = reader.readU64(); err != nil {
		return ProduceFanoutRecordResponse{}, err
	}
	if record.NextOffset, err = reader.readU64(); err != nil {
		return ProduceFanoutRecordResponse{}, err
	}
	return record, nil
}
