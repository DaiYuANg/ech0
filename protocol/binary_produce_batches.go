package protocol

func encodeProduceBatchesRequest(value any) ([]byte, error) {
	return encodeWith(value, writeProduceBatchesRequest)
}

func writeProduceBatchesRequest(writer *binaryWriter, req ProduceBatchesRequest) error {
	count, err := checkedUint32(len(req.Items), "produce batch items")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, item := range req.Items {
		if err := writeProduceBatchesItemRequest(writer, item); err != nil {
			return err
		}
	}
	return nil
}

func writeProduceBatchesItemRequest(writer *binaryWriter, item ProduceBatchesItemRequest) error {
	if err := writer.writeString(item.Topic); err != nil {
		return err
	}
	writer.writeOptionalU32(item.Partition)
	writePartitioning(writer, item.Partitioning)
	if err := writer.writeString(item.RoutingKey); err != nil {
		return err
	}
	writeProduceIdempotency(writer, item.Idempotency)
	if err := writePayloads(writer, item.Payloads); err != nil {
		return err
	}
	return writeBatchRecords(writer, item.Records)
}

func decodeProduceBatchesRequest(data []byte, target any) error {
	return decodeWith(data, target, readProduceBatchesRequest)
}

func readProduceBatchesRequest(reader *binaryReader) (ProduceBatchesRequest, error) {
	count, err := reader.readU32()
	if err != nil {
		return ProduceBatchesRequest{}, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return ProduceBatchesRequest{}, err
	}
	items := newDecodedList[ProduceBatchesItemRequest](size)
	for range size {
		item, err := readProduceBatchesItemRequest(reader)
		if err != nil {
			return ProduceBatchesRequest{}, err
		}
		items.Add(item)
	}
	return ProduceBatchesRequest{Items: items.Values()}, nil
}

func readProduceBatchesItemRequest(reader *binaryReader) (ProduceBatchesItemRequest, error) {
	topic, err := reader.readString()
	if err != nil {
		return ProduceBatchesItemRequest{}, err
	}
	item := ProduceBatchesItemRequest{Topic: topic}
	if item.Partition, err = reader.readOptionalU32(); err != nil {
		return ProduceBatchesItemRequest{}, err
	}
	if item.Partitioning, err = readPartitioning(reader); err != nil {
		return ProduceBatchesItemRequest{}, err
	}
	if item.RoutingKey, err = reader.readString(); err != nil {
		return ProduceBatchesItemRequest{}, err
	}
	if item.Idempotency, err = readProduceIdempotency(reader); err != nil {
		return ProduceBatchesItemRequest{}, err
	}
	if item.Payloads, err = readPayloads(reader); err != nil {
		return ProduceBatchesItemRequest{}, err
	}
	if item.Records, err = readBatchRecords(reader); err != nil {
		return ProduceBatchesItemRequest{}, err
	}
	return item, nil
}

func encodeProduceBatchesResponse(value any) ([]byte, error) {
	return encodeWith(value, writeProduceBatchesResponse)
}

func writeProduceBatchesResponse(writer *binaryWriter, resp ProduceBatchesResponse) error {
	count, err := checkedUint32(len(resp.Items), "produce batch response items")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, item := range resp.Items {
		if err := writeProduceBatchesItemResponse(writer, item); err != nil {
			return err
		}
	}
	return nil
}

func writeProduceBatchesItemResponse(writer *binaryWriter, item ProduceBatchesItemResponse) error {
	appended, err := checkedUint32(item.Appended, "appended")
	if err != nil {
		return err
	}
	if err := writer.writeString(item.Topic); err != nil {
		return err
	}
	writer.writeU32(item.Partition)
	writer.writeU64(item.BaseOffset)
	writer.writeU64(item.LastOffset)
	writer.writeU64(item.NextOffset)
	writer.writeU32(appended)
	return writer.writeString(item.Error)
}

func decodeProduceBatchesResponse(data []byte, target any) error {
	return decodeWith(data, target, readProduceBatchesResponse)
}

func readProduceBatchesResponse(reader *binaryReader) (ProduceBatchesResponse, error) {
	count, err := reader.readU32()
	if err != nil {
		return ProduceBatchesResponse{}, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return ProduceBatchesResponse{}, err
	}
	items := newDecodedList[ProduceBatchesItemResponse](size)
	for range size {
		item, err := readProduceBatchesItemResponse(reader)
		if err != nil {
			return ProduceBatchesResponse{}, err
		}
		items.Add(item)
	}
	return ProduceBatchesResponse{Items: items.Values()}, nil
}

func readProduceBatchesItemResponse(reader *binaryReader) (ProduceBatchesItemResponse, error) {
	var item ProduceBatchesItemResponse
	var err error
	if item.Topic, err = reader.readString(); err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	if item.Partition, err = reader.readU32(); err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	if item.BaseOffset, err = reader.readU64(); err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	if item.LastOffset, err = reader.readU64(); err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	if item.NextOffset, err = reader.readU64(); err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	appended, err := reader.readU32()
	if err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	if item.Appended, err = intFromUint32(appended); err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	if item.Error, err = reader.readString(); err != nil {
		return ProduceBatchesItemResponse{}, err
	}
	return item, nil
}
