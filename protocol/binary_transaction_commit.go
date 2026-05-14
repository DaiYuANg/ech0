package protocol

func encodeTxCommitOffsetRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req TxCommitOffsetRequest) error {
		writeTransactionIdentity(writer, req.Identity)
		return writeTxCommitOffsetFields(writer, req)
	})
}

func writeTxCommitOffsetFields(writer *binaryWriter, req TxCommitOffsetRequest) error {
	if err := writer.writeString(req.Consumer); err != nil {
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
	writer.writeU64(req.NextOffset)
	return writer.writeString(req.Metadata)
}

func decodeTxCommitOffsetRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxCommitOffsetRequest, error) {
		identity, err := readTransactionIdentity(reader)
		if err != nil {
			return TxCommitOffsetRequest{}, err
		}
		req := TxCommitOffsetRequest{Identity: identity}
		return readTxCommitOffsetFields(reader, req)
	})
}

func readTxCommitOffsetFields(reader *binaryReader, req TxCommitOffsetRequest) (TxCommitOffsetRequest, error) {
	var err error
	if req.Consumer, err = reader.readString(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	if req.Group, err = reader.readString(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	if req.MemberID, err = reader.readString(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	if req.Generation, err = reader.readU64(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	if req.Topic, err = reader.readString(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	if req.Partition, err = reader.readU32(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	if req.NextOffset, err = reader.readU64(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	if req.Metadata, err = reader.readString(); err != nil {
		return TxCommitOffsetRequest{}, err
	}
	return req, nil
}

func encodeTxCommitOffsetResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp TxCommitOffsetResponse) error {
		writer.writeU64(resp.TxID)
		if err := writer.writeString(resp.Consumer); err != nil {
			return err
		}
		if err := writer.writeString(resp.Group); err != nil {
			return err
		}
		if err := writer.writeString(resp.Topic); err != nil {
			return err
		}
		writer.writeU32(resp.Partition)
		writer.writeU64(resp.NextOffset)
		return writer.writeString(resp.Metadata)
	})
}

func decodeTxCommitOffsetResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxCommitOffsetResponse, error) {
		var resp TxCommitOffsetResponse
		var err error
		if resp.TxID, err = reader.readU64(); err != nil {
			return TxCommitOffsetResponse{}, err
		}
		return readTxCommitOffsetResponseTail(reader, resp)
	})
}

func readTxCommitOffsetResponseTail(reader *binaryReader, resp TxCommitOffsetResponse) (TxCommitOffsetResponse, error) {
	var err error
	if resp.Consumer, err = reader.readString(); err != nil {
		return TxCommitOffsetResponse{}, err
	}
	if resp.Group, err = reader.readString(); err != nil {
		return TxCommitOffsetResponse{}, err
	}
	if resp.Topic, err = reader.readString(); err != nil {
		return TxCommitOffsetResponse{}, err
	}
	if resp.Partition, err = reader.readU32(); err != nil {
		return TxCommitOffsetResponse{}, err
	}
	if resp.NextOffset, err = reader.readU64(); err != nil {
		return TxCommitOffsetResponse{}, err
	}
	if resp.Metadata, err = reader.readString(); err != nil {
		return TxCommitOffsetResponse{}, err
	}
	return resp, nil
}

func encodeTxCommitRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req TxCommitRequest) error {
		writeTransactionIdentity(writer, req.Identity)
		return nil
	})
}

func decodeTxCommitRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxCommitRequest, error) {
		identity, err := readTransactionIdentity(reader)
		return TxCommitRequest{Identity: identity}, err
	})
}

func encodeTxCommitResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp TxCommitResponse) error {
		writer.writeU64(resp.TxID)
		writeTransactionStatus(writer, resp.Status)
		return nil
	})
}

func decodeTxCommitResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxCommitResponse, error) {
		txID, err := reader.readU64()
		if err != nil {
			return TxCommitResponse{}, err
		}
		status, err := readTransactionStatus(reader)
		return TxCommitResponse{TxID: txID, Status: status}, err
	})
}

func encodeTxAbortRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req TxAbortRequest) error {
		writeTransactionIdentity(writer, req.Identity)
		return nil
	})
}

func decodeTxAbortRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxAbortRequest, error) {
		identity, err := readTransactionIdentity(reader)
		return TxAbortRequest{Identity: identity}, err
	})
}

func encodeTxAbortResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp TxAbortResponse) error {
		writer.writeU64(resp.TxID)
		writeTransactionStatus(writer, resp.Status)
		return nil
	})
}

func decodeTxAbortResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (TxAbortResponse, error) {
		txID, err := reader.readU64()
		if err != nil {
			return TxAbortResponse{}, err
		}
		status, err := readTransactionStatus(reader)
		return TxAbortResponse{TxID: txID, Status: status}, err
	})
}

func writeTransactionIdentity(writer *binaryWriter, identity TransactionIdentity) {
	writer.writeU64(identity.TxID)
	writer.writeU64(identity.ProducerID)
	writer.writeU64(identity.ProducerEpoch)
}

func readTransactionIdentity(reader *binaryReader) (TransactionIdentity, error) {
	var identity TransactionIdentity
	var err error
	if identity.TxID, err = reader.readU64(); err != nil {
		return TransactionIdentity{}, err
	}
	if identity.ProducerID, err = reader.readU64(); err != nil {
		return TransactionIdentity{}, err
	}
	if identity.ProducerEpoch, err = reader.readU64(); err != nil {
		return TransactionIdentity{}, err
	}
	return identity, nil
}
