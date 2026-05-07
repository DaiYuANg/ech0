package protocol

func encodeFetchConsumerGroupRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req FetchConsumerGroupRequest) error {
		maxRecords, err := checkedUint32(req.MaxRecords, "max_records")
		if err != nil {
			return err
		}
		if err := writer.writeString(req.Group); err != nil {
			return err
		}
		if err := writer.writeString(req.MemberID); err != nil {
			return err
		}
		writer.writeU64(req.Generation)
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
	})
}

func decodeFetchConsumerGroupRequest(data []byte, target any) error {
	return decodeWith(data, target, readFetchConsumerGroupRequest)
}

func readFetchConsumerGroupRequest(reader *binaryReader) (FetchConsumerGroupRequest, error) {
	group, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	memberID, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	req := FetchConsumerGroupRequest{Group: group, MemberID: memberID}
	if req.Generation, err = reader.readU64(); err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	if req.Topic, err = reader.readString(); err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	return readFetchConsumerGroupTail(reader, req)
}

func readFetchConsumerGroupTail(reader *binaryReader, req FetchConsumerGroupRequest) (FetchConsumerGroupRequest, error) {
	var err error
	if req.Partition, err = reader.readU32(); err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	if req.Offset, err = reader.readOptionalU64(); err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	maxRecords, err := reader.readU32()
	if err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	if req.MaxRecords, err = intFromUint32(maxRecords); err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	if req.MinRecords, err = reader.readOptionalInt(); err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	if req.MaxWaitMS, err = reader.readOptionalU64(); err != nil {
		return FetchConsumerGroupRequest{}, err
	}
	return req, nil
}

func encodeFetchConsumerGroupResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp FetchConsumerGroupResponse) error {
		if err := writer.writeString(resp.Group); err != nil {
			return err
		}
		if err := writer.writeString(resp.MemberID); err != nil {
			return err
		}
		writer.writeU64(resp.Generation)
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

func decodeFetchConsumerGroupResponse(data []byte, target any) error {
	return decodeWith(data, target, readFetchConsumerGroupResponse)
}

func readFetchConsumerGroupResponse(reader *binaryReader) (FetchConsumerGroupResponse, error) {
	group, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	memberID, err := reader.readString()
	if err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	resp := FetchConsumerGroupResponse{Group: group, MemberID: memberID}
	if resp.Generation, err = reader.readU64(); err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	if resp.Topic, err = reader.readString(); err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	return readFetchConsumerGroupResponseTail(reader, resp)
}

func readFetchConsumerGroupResponseTail(reader *binaryReader, resp FetchConsumerGroupResponse) (FetchConsumerGroupResponse, error) {
	var err error
	if resp.Partition, err = reader.readU32(); err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	if resp.Records, err = readFetchRecords(reader); err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	if resp.HighWatermark, err = reader.readOptionalU64(); err != nil {
		return FetchConsumerGroupResponse{}, err
	}
	return resp, nil
}
