package protocol

func encodeHandshakeRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req HandshakeRequest) error {
		if err := writer.writeString(req.ClientID); err != nil {
			return err
		}
		if err := writer.writeString(req.Tenant); err != nil {
			return err
		}
		if err := writer.writeString(req.Namespace); err != nil {
			return err
		}
		if err := writer.writeString(req.Principal); err != nil {
			return err
		}
		if err := writer.writeString(req.AuthToken); err != nil {
			return err
		}
		return writeStringSlice(writer, req.Capabilities)
	})
}

func decodeHandshakeRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (HandshakeRequest, error) {
		clientID, err := reader.readString()
		if err != nil {
			return HandshakeRequest{}, err
		}
		tenant, err := reader.readString()
		if err != nil {
			return HandshakeRequest{}, err
		}
		namespace, err := reader.readString()
		if err != nil {
			return HandshakeRequest{}, err
		}
		principal, err := reader.readString()
		if err != nil {
			return HandshakeRequest{}, err
		}
		authToken, err := reader.readString()
		if err != nil {
			return HandshakeRequest{}, err
		}
		capabilities, err := readStringSlice(reader)
		return HandshakeRequest{
			ClientID:     clientID,
			Tenant:       tenant,
			Namespace:    namespace,
			Principal:    principal,
			AuthToken:    authToken,
			Capabilities: capabilities,
		}, err
	})
}

func encodeHandshakeResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp HandshakeResponse) error {
		if err := writer.writeString(resp.ServerID); err != nil {
			return err
		}
		writer.writeU8(resp.ProtocolVersion)
		if err := writer.writeString(resp.Tenant); err != nil {
			return err
		}
		if err := writer.writeString(resp.Namespace); err != nil {
			return err
		}
		if err := writer.writeString(resp.Principal); err != nil {
			return err
		}
		return writeStringSlice(writer, resp.Capabilities)
	})
}

func decodeHandshakeResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (HandshakeResponse, error) {
		serverID, err := reader.readString()
		if err != nil {
			return HandshakeResponse{}, err
		}
		version, err := reader.readU8()
		if err != nil {
			return HandshakeResponse{}, err
		}
		tenant, err := reader.readString()
		if err != nil {
			return HandshakeResponse{}, err
		}
		namespace, err := reader.readString()
		if err != nil {
			return HandshakeResponse{}, err
		}
		principal, err := reader.readString()
		if err != nil {
			return HandshakeResponse{}, err
		}
		capabilities, err := readStringSlice(reader)
		return HandshakeResponse{
			ServerID:        serverID,
			ProtocolVersion: version,
			Tenant:          tenant,
			Namespace:       namespace,
			Principal:       principal,
			Capabilities:    capabilities,
		}, err
	})
}

func encodePingRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req PingRequest) error {
		writer.writeU64(req.Nonce)
		return nil
	})
}

func decodePingRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (PingRequest, error) {
		nonce, err := reader.readU64()
		return PingRequest{Nonce: nonce}, err
	})
}

func encodePingResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp PingResponse) error {
		writer.writeU64(resp.Nonce)
		return nil
	})
}

func decodePingResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (PingResponse, error) {
		nonce, err := reader.readU64()
		return PingResponse{Nonce: nonce}, err
	})
}
