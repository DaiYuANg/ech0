package protocol

func encodeStartRequestRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req StartRequestRequest) error {
		if err := writer.writeString(req.Subject); err != nil {
			return err
		}
		if err := writer.writeString(req.InstanceID); err != nil {
			return err
		}
		writer.writeOptionalU64(req.TimeoutMS)
		writer.writeOptionalU64(req.PollIntervalMS)
		writer.writeOptionalU32(req.Partition)
		writePartitioning(writer, req.Partitioning)
		if err := writeHeaders(writer, req.Headers); err != nil {
			return err
		}
		return writer.writeBytes(req.Payload)
	})
}

func decodeStartRequestRequest(data []byte, target any) error {
	return decodeWith(data, target, readStartRequestRequest)
}

func readStartRequestRequest(reader *binaryReader) (StartRequestRequest, error) {
	subject, err := reader.readString()
	if err != nil {
		return StartRequestRequest{}, err
	}
	instanceID, err := reader.readString()
	if err != nil {
		return StartRequestRequest{}, err
	}
	req := StartRequestRequest{Subject: subject, InstanceID: instanceID}
	return readStartRequestRequestTail(reader, req)
}

func readStartRequestRequestTail(reader *binaryReader, req StartRequestRequest) (StartRequestRequest, error) {
	var err error
	if req.TimeoutMS, err = reader.readOptionalU64(); err != nil {
		return StartRequestRequest{}, err
	}
	if req.PollIntervalMS, err = reader.readOptionalU64(); err != nil {
		return StartRequestRequest{}, err
	}
	if req.Partition, err = reader.readOptionalU32(); err != nil {
		return StartRequestRequest{}, err
	}
	if req.Partitioning, err = readPartitioning(reader); err != nil {
		return StartRequestRequest{}, err
	}
	if req.Headers, err = readHeaders(reader); err != nil {
		return StartRequestRequest{}, err
	}
	if req.Payload, err = reader.readBytes(); err != nil {
		return StartRequestRequest{}, err
	}
	return req, nil
}

func encodeStartRequestResponse(value any) ([]byte, error) {
	return encodeWith(value, writeStartRequestResponse)
}

func writeStartRequestResponse(writer *binaryWriter, resp StartRequestResponse) error {
	if err := writer.writeString(resp.Subject); err != nil {
		return err
	}
	if err := writer.writeString(resp.InstanceID); err != nil {
		return err
	}
	if err := writer.writeString(resp.ReplyTo); err != nil {
		return err
	}
	if err := writer.writeString(resp.CorrelationID); err != nil {
		return err
	}
	writer.writeU64(resp.ExpiresAtMS)
	writer.writeU32(resp.Partition)
	writer.writeU64(resp.Offset)
	writer.writeU64(resp.NextOffset)
	return nil
}

func decodeStartRequestResponse(data []byte, target any) error {
	return decodeWith(data, target, readStartRequestResponse)
}

func readStartRequestResponse(reader *binaryReader) (StartRequestResponse, error) {
	subject, err := reader.readString()
	if err != nil {
		return StartRequestResponse{}, err
	}
	instanceID, err := reader.readString()
	if err != nil {
		return StartRequestResponse{}, err
	}
	resp := StartRequestResponse{Subject: subject, InstanceID: instanceID}
	return readStartRequestResponseTail(reader, resp)
}

func readStartRequestResponseTail(reader *binaryReader, resp StartRequestResponse) (StartRequestResponse, error) {
	var err error
	if resp.ReplyTo, err = reader.readString(); err != nil {
		return StartRequestResponse{}, err
	}
	if resp.CorrelationID, err = reader.readString(); err != nil {
		return StartRequestResponse{}, err
	}
	if resp.ExpiresAtMS, err = reader.readU64(); err != nil {
		return StartRequestResponse{}, err
	}
	if resp.Partition, err = reader.readU32(); err != nil {
		return StartRequestResponse{}, err
	}
	if resp.Offset, err = reader.readU64(); err != nil {
		return StartRequestResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return StartRequestResponse{}, err
	}
	return resp, nil
}
