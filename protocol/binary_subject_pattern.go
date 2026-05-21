package protocol

func encodeFetchSubjectPatternRequest(value any) ([]byte, error) {
	return encodeWith(value, writeFetchSubjectPatternRequest)
}

func writeFetchSubjectPatternRequest(writer *binaryWriter, req FetchSubjectPatternRequest) error {
	maxRecords, err := checkedUint32(req.MaxRecords, "max_records")
	if err != nil {
		return err
	}
	if err := writer.writeString(req.Consumer); err != nil {
		return err
	}
	if err := writer.writeString(req.Pattern); err != nil {
		return err
	}
	writer.writeU32(maxRecords)
	writeFetchIsolation(writer, req.Isolation)
	return nil
}

func decodeFetchSubjectPatternRequest(data []byte, target any) error {
	return decodeWith(data, target, readFetchSubjectPatternRequest)
}

func readFetchSubjectPatternRequest(reader *binaryReader) (FetchSubjectPatternRequest, error) {
	consumer, err := reader.readString()
	if err != nil {
		return FetchSubjectPatternRequest{}, err
	}
	pattern, err := reader.readString()
	if err != nil {
		return FetchSubjectPatternRequest{}, err
	}
	maxRecords, err := reader.readU32()
	if err != nil {
		return FetchSubjectPatternRequest{}, err
	}
	req := FetchSubjectPatternRequest{Consumer: consumer, Pattern: pattern}
	if req.MaxRecords, err = intFromUint32(maxRecords); err != nil {
		return FetchSubjectPatternRequest{}, err
	}
	if req.Isolation, err = readFetchIsolation(reader); err != nil {
		return FetchSubjectPatternRequest{}, err
	}
	return req, nil
}

func encodeFetchSubjectPatternResponse(value any) ([]byte, error) {
	return encodeWith(value, writeFetchSubjectPatternResponse)
}

func writeFetchSubjectPatternResponse(writer *binaryWriter, resp FetchSubjectPatternResponse) error {
	if err := writer.writeString(resp.Pattern); err != nil {
		return err
	}
	count, err := checkedUint32(len(resp.Items), "subject pattern items")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for index := range resp.Items {
		if err := writeFetchBatchItemResponse(writer, resp.Items[index]); err != nil {
			return err
		}
	}
	return nil
}

func decodeFetchSubjectPatternResponse(data []byte, target any) error {
	return decodeWith(data, target, readFetchSubjectPatternResponse)
}

func readFetchSubjectPatternResponse(reader *binaryReader) (FetchSubjectPatternResponse, error) {
	pattern, err := reader.readString()
	if err != nil {
		return FetchSubjectPatternResponse{}, err
	}
	count, err := reader.readU32()
	if err != nil {
		return FetchSubjectPatternResponse{}, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return FetchSubjectPatternResponse{}, err
	}
	items := newDecodedList[FetchSubjectPatternItemResponse](size)
	for range size {
		item, err := readFetchBatchItemResponse(reader)
		if err != nil {
			return FetchSubjectPatternResponse{}, err
		}
		items.Add(item)
	}
	return FetchSubjectPatternResponse{Pattern: pattern, Items: items.Values()}, nil
}
