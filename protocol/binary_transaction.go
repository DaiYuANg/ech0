package protocol

func encodeTxBeginRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req TxBeginRequest) error {
		if err := writer.writeString(req.TransactionalID); err != nil {
			return err
		}
		writer.writeU64(req.TimeoutMS)
		return nil
	})
}

func decodeTxBeginRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxBeginRequest, error) {
		transactionalID, err := reader.readString()
		if err != nil {
			return TxBeginRequest{}, err
		}
		timeoutMS, err := reader.readU64()
		return TxBeginRequest{TransactionalID: transactionalID, TimeoutMS: timeoutMS}, err
	})
}

func encodeTxBeginResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp TxBeginResponse) error {
		writeTransactionIdentity(writer, resp.Identity)
		writer.writeU64(resp.ExpiresAtMS)
		writeTransactionStatus(writer, resp.Status)
		return nil
	})
}

func decodeTxBeginResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxBeginResponse, error) {
		identity, err := readTransactionIdentity(reader)
		if err != nil {
			return TxBeginResponse{}, err
		}
		expiresAtMS, err := reader.readU64()
		if err != nil {
			return TxBeginResponse{}, err
		}
		status, err := readTransactionStatus(reader)
		return TxBeginResponse{Identity: identity, ExpiresAtMS: expiresAtMS, Status: status}, err
	})
}

func encodeTxPublishRequest(value any) ([]byte, error) {
	return encodeWith(value, writeTxPublishRequest)
}

func writeTxPublishRequest(writer *binaryWriter, req TxPublishRequest) error {
	writeTransactionIdentity(writer, req.Identity)
	writer.writeU64(req.Sequence)
	if err := writer.writeString(req.Topic); err != nil {
		return err
	}
	writer.writeOptionalU32(req.Partition)
	writePartitioning(writer, req.Partitioning)
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
	return writer.writeBytes(req.Payload)
}

func decodeTxPublishRequest(data []byte, target any) error {
	return decodeWith(data, target, readTxPublishRequest)
}

func readTxPublishRequest(reader *binaryReader) (TxPublishRequest, error) {
	identity, err := readTransactionIdentity(reader)
	if err != nil {
		return TxPublishRequest{}, err
	}
	req := TxPublishRequest{Identity: identity}
	if req.Sequence, err = reader.readU64(); err != nil {
		return TxPublishRequest{}, err
	}
	if req.Topic, err = reader.readString(); err != nil {
		return TxPublishRequest{}, err
	}
	return readTxPublishRequestTail(reader, req)
}

func readTxPublishRequestTail(reader *binaryReader, req TxPublishRequest) (TxPublishRequest, error) {
	var err error
	if req.Partition, err = reader.readOptionalU32(); err != nil {
		return TxPublishRequest{}, err
	}
	if req.Partitioning, err = readPartitioning(reader); err != nil {
		return TxPublishRequest{}, err
	}
	if req.RoutingKey, err = reader.readString(); err != nil {
		return TxPublishRequest{}, err
	}
	if req.Key, err = reader.readBytes(); err != nil {
		return TxPublishRequest{}, err
	}
	if req.Headers, err = readHeaders(reader); err != nil {
		return TxPublishRequest{}, err
	}
	if req.Tombstone, err = reader.readBool(); err != nil {
		return TxPublishRequest{}, err
	}
	if req.Payload, err = reader.readBytes(); err != nil {
		return TxPublishRequest{}, err
	}
	return req, nil
}

func encodeTxPublishResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp TxPublishResponse) error {
		writer.writeU64(resp.TxID)
		writer.writeU32(resp.Partition)
		writer.writeU64(resp.Offset)
		writer.writeU64(resp.NextOffset)
		return nil
	})
}

func decodeTxPublishResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxPublishResponse, error) {
		var resp TxPublishResponse
		var err error
		if resp.TxID, err = reader.readU64(); err != nil {
			return TxPublishResponse{}, err
		}
		if resp.Partition, err = reader.readU32(); err != nil {
			return TxPublishResponse{}, err
		}
		if resp.Offset, err = reader.readU64(); err != nil {
			return TxPublishResponse{}, err
		}
		if resp.NextOffset, err = reader.readU64(); err != nil {
			return TxPublishResponse{}, err
		}
		return resp, nil
	})
}

func encodeTxPublishBatchRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req TxPublishBatchRequest) error {
		writeTransactionIdentity(writer, req.Identity)
		writer.writeU64(req.BaseSequence)
		return writeProduceBatchRequest(writer, ProduceBatchRequest{
			Topic: req.Topic, Partition: req.Partition, Partitioning: req.Partitioning,
			RoutingKey: req.RoutingKey, Payloads: req.Payloads, Records: req.Records,
		})
	})
}

func decodeTxPublishBatchRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxPublishBatchRequest, error) {
		identity, err := readTransactionIdentity(reader)
		if err != nil {
			return TxPublishBatchRequest{}, err
		}
		baseSequence, err := reader.readU64()
		if err != nil {
			return TxPublishBatchRequest{}, err
		}
		batch, err := readProduceBatchRequest(reader)
		return TxPublishBatchRequest{
			Identity: identity, BaseSequence: baseSequence,
			Topic: batch.Topic, Partition: batch.Partition, Partitioning: batch.Partitioning,
			RoutingKey: batch.RoutingKey, Payloads: batch.Payloads, Records: batch.Records,
		}, err
	})
}

func encodeTxPublishBatchResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp TxPublishBatchResponse) error {
		appended, err := checkedUint32(resp.Appended, "appended")
		if err != nil {
			return err
		}
		writer.writeU64(resp.TxID)
		writer.writeU32(resp.Partition)
		writer.writeU64(resp.BaseOffset)
		writer.writeU64(resp.LastOffset)
		writer.writeU64(resp.NextOffset)
		writer.writeU32(appended)
		return nil
	})
}

func decodeTxPublishBatchResponse(data []byte, target any) error {
	return decodeWith(data, target, readTxPublishBatchResponse)
}

func readTxPublishBatchResponse(reader *binaryReader) (TxPublishBatchResponse, error) {
	var resp TxPublishBatchResponse
	var err error
	if resp.TxID, err = reader.readU64(); err != nil {
		return TxPublishBatchResponse{}, err
	}
	if resp.Partition, err = reader.readU32(); err != nil {
		return TxPublishBatchResponse{}, err
	}
	return readTxPublishBatchResponseTail(reader, resp)
}

func readTxPublishBatchResponseTail(reader *binaryReader, resp TxPublishBatchResponse) (TxPublishBatchResponse, error) {
	var err error
	if resp.BaseOffset, err = reader.readU64(); err != nil {
		return TxPublishBatchResponse{}, err
	}
	if resp.LastOffset, err = reader.readU64(); err != nil {
		return TxPublishBatchResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return TxPublishBatchResponse{}, err
	}
	appended, err := reader.readU32()
	if err != nil {
		return TxPublishBatchResponse{}, err
	}
	resp.Appended, err = intFromUint32(appended)
	return resp, err
}
