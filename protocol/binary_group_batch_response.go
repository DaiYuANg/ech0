package protocol

func encodeFetchConsumerGroupBatchResponse(value any) ([]byte, error) {
	return encodeWith(value, writeFetchConsumerGroupBatchResponse)
}

func writeFetchConsumerGroupBatchResponse(writer *binaryWriter, resp FetchConsumerGroupBatchResponse) error {
	if err := writer.writeString(resp.Group); err != nil {
		return err
	}
	if err := writer.writeString(resp.MemberID); err != nil {
		return err
	}
	writer.writeU64(resp.Generation)
	return writeConsumerGroupBatchItems(writer, resp.Items)
}

func decodeFetchConsumerGroupBatchResponse(data []byte, target any) error {
	return decodeWith(data, target, readFetchConsumerGroupBatchResponse)
}

func readFetchConsumerGroupBatchResponse(reader *binaryReader) (FetchConsumerGroupBatchResponse, error) {
	group, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupBatchResponse{}, err
	}
	memberID, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupBatchResponse{}, err
	}
	resp := FetchConsumerGroupBatchResponse{Group: group, MemberID: memberID}
	if resp.Generation, err = reader.readU64(); err != nil {
		return FetchConsumerGroupBatchResponse{}, err
	}
	if resp.Items, err = readConsumerGroupBatchItems(reader); err != nil {
		return FetchConsumerGroupBatchResponse{}, err
	}
	return resp, nil
}

func writeConsumerGroupBatchItem(writer *binaryWriter, item FetchConsumerGroupBatchItemResponse) error {
	if err := writer.writeString(item.Topic); err != nil {
		return err
	}
	writer.writeU32(item.Partition)
	if err := writeFetchRecords(writer, item.Records); err != nil {
		return err
	}
	writer.writeU64(item.NextOffset)
	writeFetchWatermarks(writer, item.HighWatermark, item.LowWatermark, item.LogStartOffset)
	return nil
}

func writeConsumerGroupBatchItems(writer *binaryWriter, items []FetchConsumerGroupBatchItemResponse) error {
	count, err := checkedUint32(len(items), "consumer_group_batch_items")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, item := range items {
		if err := writeConsumerGroupBatchItem(writer, item); err != nil {
			return err
		}
	}
	return nil
}

func readConsumerGroupBatchItems(reader *binaryReader) ([]FetchConsumerGroupBatchItemResponse, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	items := newDecodedList[FetchConsumerGroupBatchItemResponse](size)
	for range size {
		item, err := readConsumerGroupBatchItem(reader)
		if err != nil {
			return nil, err
		}
		items.Add(item)
	}
	return items.Values(), nil
}

func readConsumerGroupBatchItem(reader *binaryReader) (FetchConsumerGroupBatchItemResponse, error) {
	topic, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupBatchItemResponse{}, err
	}
	item := FetchConsumerGroupBatchItemResponse{Topic: topic}
	if item.Partition, err = reader.readU32(); err != nil {
		return FetchConsumerGroupBatchItemResponse{}, err
	}
	if item.Records, err = readFetchRecords(reader); err != nil {
		return FetchConsumerGroupBatchItemResponse{}, err
	}
	if item.NextOffset, err = reader.readU64(); err != nil {
		return FetchConsumerGroupBatchItemResponse{}, err
	}
	if err := readFetchWatermarks(reader, &item.HighWatermark, &item.LowWatermark, &item.LogStartOffset); err != nil {
		return FetchConsumerGroupBatchItemResponse{}, err
	}
	return item, nil
}
