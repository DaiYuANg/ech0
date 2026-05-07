package protocol

func encodeAckDirectRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req AckDirectRequest) error {
		if err := writer.writeString(req.Recipient); err != nil {
			return err
		}
		writer.writeU64(req.NextOffset)
		return nil
	})
}

func decodeAckDirectRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (AckDirectRequest, error) {
		recipient, err := reader.readString()
		if err != nil {
			return AckDirectRequest{}, err
		}
		nextOffset, err := reader.readU64()
		return AckDirectRequest{Recipient: recipient, NextOffset: nextOffset}, err
	})
}

func encodeAckDirectResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp AckDirectResponse) error {
		if err := writer.writeString(resp.Recipient); err != nil {
			return err
		}
		writer.writeU64(resp.NextOffset)
		return nil
	})
}

func decodeAckDirectResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (AckDirectResponse, error) {
		recipient, err := reader.readString()
		if err != nil {
			return AckDirectResponse{}, err
		}
		nextOffset, err := reader.readU64()
		return AckDirectResponse{Recipient: recipient, NextOffset: nextOffset}, err
	})
}
