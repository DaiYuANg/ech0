package protocol

func encodeFetchBatchRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req FetchBatchRequest) error {
		if err := writer.writeString(req.Consumer); err != nil {
			return err
		}
		if err := writeFetchBatchItems(writer, req.Items); err != nil {
			return err
		}
		if err := writer.writeOptionalInt(req.MinRecords); err != nil {
			return err
		}
		writer.writeOptionalU64(req.MaxWaitMS)
		writeFetchIsolation(writer, req.Isolation)
		return nil
	})
}

func decodeFetchBatchRequest(data []byte, target any) error {
	return decodeWith(data, target, readFetchBatchRequest)
}

func readFetchBatchRequest(reader *binaryReader) (FetchBatchRequest, error) {
	consumer, err := reader.readString()
	if err != nil {
		return FetchBatchRequest{}, err
	}
	req := FetchBatchRequest{Consumer: consumer}
	if req.Items, err = readFetchBatchItems(reader); err != nil {
		return FetchBatchRequest{}, err
	}
	if req.MinRecords, err = reader.readOptionalInt(); err != nil {
		return FetchBatchRequest{}, err
	}
	if req.MaxWaitMS, err = reader.readOptionalU64(); err != nil {
		return FetchBatchRequest{}, err
	}
	if req.Isolation, err = readFetchIsolation(reader); err != nil {
		return FetchBatchRequest{}, err
	}
	return req, nil
}

func writeFetchBatchItems(writer *binaryWriter, items []FetchBatchItemRequest) error {
	count, err := checkedUint32(len(items), "fetch_batch_items")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, item := range items {
		if err := writeFetchBatchItem(writer, item); err != nil {
			return err
		}
	}
	return nil
}

func readFetchBatchItems(reader *binaryReader) ([]FetchBatchItemRequest, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	items := newDecodedList[FetchBatchItemRequest](size)
	for range size {
		item, err := readFetchBatchItem(reader)
		if err != nil {
			return nil, err
		}
		items.Add(item)
	}
	return items.Values(), nil
}

func writeFetchBatchItem(writer *binaryWriter, item FetchBatchItemRequest) error {
	maxRecords, err := checkedUint32(item.MaxRecords, "max_records")
	if err != nil {
		return err
	}
	if err := writer.writeString(item.Topic); err != nil {
		return err
	}
	writer.writeU32(item.Partition)
	writer.writeOptionalU64(item.Offset)
	writer.writeU32(maxRecords)
	return nil
}

func readFetchBatchItem(reader *binaryReader) (FetchBatchItemRequest, error) {
	topic, err := reader.readString()
	if err != nil {
		return FetchBatchItemRequest{}, err
	}
	item := FetchBatchItemRequest{Topic: topic}
	if item.Partition, err = reader.readU32(); err != nil {
		return FetchBatchItemRequest{}, err
	}
	if item.Offset, err = reader.readOptionalU64(); err != nil {
		return FetchBatchItemRequest{}, err
	}
	maxRecords, err := reader.readU32()
	if err != nil {
		return FetchBatchItemRequest{}, err
	}
	item.MaxRecords, err = intFromUint32(maxRecords)
	return item, err
}

func encodeFetchBatchResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp FetchBatchResponse) error {
		count, err := checkedUint32(len(resp.Items), "fetch_batch_response_items")
		if err != nil {
			return err
		}
		writer.writeU32(count)
		for _, item := range resp.Items {
			if err := writeFetchBatchItemResponse(writer, item); err != nil {
				return err
			}
		}
		return nil
	})
}

func decodeFetchBatchResponse(data []byte, target any) error {
	return decodeWith(data, target, readFetchBatchResponse)
}

func readFetchBatchResponse(reader *binaryReader) (FetchBatchResponse, error) {
	count, err := reader.readU32()
	if err != nil {
		return FetchBatchResponse{}, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return FetchBatchResponse{}, err
	}
	items := newDecodedList[FetchBatchItemResponse](size)
	for range size {
		item, err := readFetchBatchItemResponse(reader)
		if err != nil {
			return FetchBatchResponse{}, err
		}
		items.Add(item)
	}
	return FetchBatchResponse{Items: items.Values()}, nil
}

func writeFetchBatchItemResponse(writer *binaryWriter, item FetchBatchItemResponse) error {
	if err := writer.writeString(item.Topic); err != nil {
		return err
	}
	writer.writeU32(item.Partition)
	if err := writeFetchRecords(writer, item.Records); err != nil {
		return err
	}
	writer.writeU64(item.NextOffset)
	writer.writeOptionalU64(item.HighWatermark)
	return nil
}

func readFetchBatchItemResponse(reader *binaryReader) (FetchBatchItemResponse, error) {
	topic, err := reader.readString()
	if err != nil {
		return FetchBatchItemResponse{}, err
	}
	item := FetchBatchItemResponse{Topic: topic}
	if item.Partition, err = reader.readU32(); err != nil {
		return FetchBatchItemResponse{}, err
	}
	if item.Records, err = readFetchRecords(reader); err != nil {
		return FetchBatchItemResponse{}, err
	}
	if item.NextOffset, err = reader.readU64(); err != nil {
		return FetchBatchItemResponse{}, err
	}
	if item.HighWatermark, err = reader.readOptionalU64(); err != nil {
		return FetchBatchItemResponse{}, err
	}
	return item, nil
}
