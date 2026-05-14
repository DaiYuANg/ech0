package protocol

func encodeJoinConsumerGroupRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req JoinConsumerGroupRequest) error {
		if err := writer.writeString(req.Group); err != nil {
			return err
		}
		if err := writer.writeString(req.MemberID); err != nil {
			return err
		}
		if err := writeStringSlice(writer, req.Topics); err != nil {
			return err
		}
		writer.writeU64(req.SessionTimeoutMS)
		writer.writeU64(req.MaxPollIntervalMS)
		return nil
	})
}

func decodeJoinConsumerGroupRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (JoinConsumerGroupRequest, error) {
		group, err := reader.readString()
		if err != nil {
			return JoinConsumerGroupRequest{}, err
		}
		memberID, err := reader.readString()
		if err != nil {
			return JoinConsumerGroupRequest{}, err
		}
		topics, err := readStringSlice(reader)
		if err != nil {
			return JoinConsumerGroupRequest{}, err
		}
		timeout, err := reader.readU64()
		if err != nil {
			return JoinConsumerGroupRequest{}, err
		}
		maxPollInterval, err := reader.readU64()
		return JoinConsumerGroupRequest{Group: group, MemberID: memberID, Topics: topics, SessionTimeoutMS: timeout, MaxPollIntervalMS: maxPollInterval}, err
	})
}

func encodeJoinConsumerGroupResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp JoinConsumerGroupResponse) error {
		return writeLease(writer, resp.Lease)
	})
}

func decodeJoinConsumerGroupResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (JoinConsumerGroupResponse, error) {
		lease, err := readLease(reader)
		return JoinConsumerGroupResponse{Lease: lease}, err
	})
}

func encodeHeartbeatConsumerGroupRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req HeartbeatConsumerGroupRequest) error {
		if err := writer.writeString(req.Group); err != nil {
			return err
		}
		if err := writer.writeString(req.MemberID); err != nil {
			return err
		}
		writer.writeOptionalU64(req.SessionTimeoutMS)
		writer.writeOptionalU64(req.MaxPollIntervalMS)
		return nil
	})
}

func decodeHeartbeatConsumerGroupRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (HeartbeatConsumerGroupRequest, error) {
		group, err := reader.readString()
		if err != nil {
			return HeartbeatConsumerGroupRequest{}, err
		}
		memberID, err := reader.readString()
		if err != nil {
			return HeartbeatConsumerGroupRequest{}, err
		}
		timeout, err := reader.readOptionalU64()
		if err != nil {
			return HeartbeatConsumerGroupRequest{}, err
		}
		maxPollInterval, err := reader.readOptionalU64()
		return HeartbeatConsumerGroupRequest{Group: group, MemberID: memberID, SessionTimeoutMS: timeout, MaxPollIntervalMS: maxPollInterval}, err
	})
}

func encodeHeartbeatConsumerGroupResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp HeartbeatConsumerGroupResponse) error {
		return writeLease(writer, resp.Lease)
	})
}

func decodeHeartbeatConsumerGroupResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (HeartbeatConsumerGroupResponse, error) {
		lease, err := readLease(reader)
		return HeartbeatConsumerGroupResponse{Lease: lease}, err
	})
}

func writeLease(writer *binaryWriter, lease ConsumerGroupMemberLease) error {
	if err := writer.writeString(lease.Group); err != nil {
		return err
	}
	if err := writer.writeString(lease.MemberID); err != nil {
		return err
	}
	if err := writeStringSlice(writer, lease.Topics); err != nil {
		return err
	}
	writer.writeU64(lease.SessionTimeoutMS)
	writer.writeU64(lease.MaxPollIntervalMS)
	writer.writeU64(lease.JoinedAtMS)
	writer.writeU64(lease.LastHeartbeatMS)
	writer.writeU64(lease.LastPollMS)
	writer.writeU64(lease.ExpiresAtMS)
	writer.writeU64(lease.PollExpiresAtMS)
	return nil
}

func readLease(reader *binaryReader) (ConsumerGroupMemberLease, error) {
	group, err := reader.readString()
	if err != nil {
		return ConsumerGroupMemberLease{}, err
	}
	memberID, err := reader.readString()
	if err != nil {
		return ConsumerGroupMemberLease{}, err
	}
	topics, err := readStringSlice(reader)
	if err != nil {
		return ConsumerGroupMemberLease{}, err
	}
	lease := ConsumerGroupMemberLease{Group: group, MemberID: memberID, Topics: topics}
	fields, err := readLeaseTimingFields(reader)
	if err != nil {
		return ConsumerGroupMemberLease{}, err
	}
	lease.SessionTimeoutMS = fields[0]
	lease.MaxPollIntervalMS = fields[1]
	lease.JoinedAtMS = fields[2]
	lease.LastHeartbeatMS = fields[3]
	lease.LastPollMS = fields[4]
	lease.ExpiresAtMS = fields[5]
	lease.PollExpiresAtMS = fields[6]
	return lease, nil
}

func readLeaseTimingFields(reader *binaryReader) ([7]uint64, error) {
	var fields [7]uint64
	for index := range fields {
		value, err := reader.readU64()
		if err != nil {
			return fields, err
		}
		fields[index] = value
	}
	return fields, nil
}

func encodeRebalanceConsumerGroupRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req RebalanceConsumerGroupRequest) error {
		return writer.writeString(req.Group)
	})
}

func decodeRebalanceConsumerGroupRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (RebalanceConsumerGroupRequest, error) {
		group, err := reader.readString()
		return RebalanceConsumerGroupRequest{Group: group}, err
	})
}

func encodeRebalanceConsumerGroupResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp RebalanceConsumerGroupResponse) error {
		return writeAssignment(writer, resp.Assignment)
	})
}

func decodeRebalanceConsumerGroupResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (RebalanceConsumerGroupResponse, error) {
		assignment, err := readAssignment(reader)
		return RebalanceConsumerGroupResponse{Assignment: assignment}, err
	})
}
