package protocol

func encodeProcessRetryRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req ProcessRetryRequest) error {
		maxRecords, err := checkedUint32(req.MaxRecords, "max_records")
		if err != nil {
			return err
		}
		if err := writer.writeString(req.Consumer); err != nil {
			return err
		}
		if err := writer.writeString(req.SourceTopic); err != nil {
			return err
		}
		writer.writeU32(req.Partition)
		writer.writeU32(maxRecords)
		return nil
	})
}

func decodeProcessRetryRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ProcessRetryRequest, error) {
		consumer, err := reader.readString()
		if err != nil {
			return ProcessRetryRequest{}, err
		}
		sourceTopic, err := reader.readString()
		if err != nil {
			return ProcessRetryRequest{}, err
		}
		req := ProcessRetryRequest{Consumer: consumer, SourceTopic: sourceTopic}
		if req.Partition, err = reader.readU32(); err != nil {
			return ProcessRetryRequest{}, err
		}
		maxRecords, err := reader.readU32()
		if err != nil {
			return ProcessRetryRequest{}, err
		}
		req.MaxRecords, err = intFromUint32(maxRecords)
		return req, err
	})
}

func encodeProcessRetryResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp ProcessRetryResponse) error {
		movedToOrigin, err := checkedUint32(resp.MovedToOrigin, "moved_to_origin")
		if err != nil {
			return err
		}
		movedToDeadLetter, err := checkedUint32(resp.MovedToDeadLetter, "moved_to_dead_letter")
		if err != nil {
			return err
		}
		if err := writer.writeString(resp.RetryTopic); err != nil {
			return err
		}
		writer.writeU32(resp.Partition)
		writer.writeU32(movedToOrigin)
		writer.writeU32(movedToDeadLetter)
		writer.writeOptionalU64(resp.CommittedNextOffset)
		return nil
	})
}

func decodeProcessRetryResponse(data []byte, target any) error {
	return decodeWith(data, target, readProcessRetryResponse)
}

func readProcessRetryResponse(reader *binaryReader) (ProcessRetryResponse, error) {
	topic, err := reader.readString()
	if err != nil {
		return ProcessRetryResponse{}, err
	}
	resp := ProcessRetryResponse{RetryTopic: topic}
	if resp.Partition, err = reader.readU32(); err != nil {
		return ProcessRetryResponse{}, err
	}
	movedToOrigin, err := reader.readU32()
	if err != nil {
		return ProcessRetryResponse{}, err
	}
	if resp.MovedToOrigin, err = intFromUint32(movedToOrigin); err != nil {
		return ProcessRetryResponse{}, err
	}
	movedToDeadLetter, err := reader.readU32()
	if err != nil {
		return ProcessRetryResponse{}, err
	}
	if resp.MovedToDeadLetter, err = intFromUint32(movedToDeadLetter); err != nil {
		return ProcessRetryResponse{}, err
	}
	if resp.CommittedNextOffset, err = reader.readOptionalU64(); err != nil {
		return ProcessRetryResponse{}, err
	}
	return resp, nil
}

func encodeScheduleDelayRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req ScheduleDelayRequest) error {
		if err := writer.writeString(req.Topic); err != nil {
			return err
		}
		writer.writeU32(req.Partition)
		if err := writer.writeBytes(req.Payload); err != nil {
			return err
		}
		writer.writeU64(req.DeliverAtMS)
		return nil
	})
}

func decodeScheduleDelayRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ScheduleDelayRequest, error) {
		topic, err := reader.readString()
		if err != nil {
			return ScheduleDelayRequest{}, err
		}
		req := ScheduleDelayRequest{Topic: topic}
		if req.Partition, err = reader.readU32(); err != nil {
			return ScheduleDelayRequest{}, err
		}
		if req.Payload, err = reader.readBytes(); err != nil {
			return ScheduleDelayRequest{}, err
		}
		if req.DeliverAtMS, err = reader.readU64(); err != nil {
			return ScheduleDelayRequest{}, err
		}
		return req, nil
	})
}

func encodeScheduleDelayResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp ScheduleDelayResponse) error {
		if err := writer.writeString(resp.DelayTopic); err != nil {
			return err
		}
		writer.writeU32(resp.Partition)
		writer.writeU64(resp.Offset)
		writer.writeU64(resp.NextOffset)
		writer.writeU64(resp.DeliverAtMS)
		return nil
	})
}

func decodeScheduleDelayResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (ScheduleDelayResponse, error) {
		topic, err := reader.readString()
		if err != nil {
			return ScheduleDelayResponse{}, err
		}
		resp := ScheduleDelayResponse{DelayTopic: topic}
		if resp.Partition, err = reader.readU32(); err != nil {
			return ScheduleDelayResponse{}, err
		}
		if resp.Offset, err = reader.readU64(); err != nil {
			return ScheduleDelayResponse{}, err
		}
		if resp.NextOffset, err = reader.readU64(); err != nil {
			return ScheduleDelayResponse{}, err
		}
		if resp.DeliverAtMS, err = reader.readU64(); err != nil {
			return ScheduleDelayResponse{}, err
		}
		return resp, nil
	})
}
