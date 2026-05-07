package protocol

func encodeGetConsumerGroupAssignmentRequest(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, req GetConsumerGroupAssignmentRequest) error {
		return writer.writeString(req.Group)
	})
}

func decodeGetConsumerGroupAssignmentRequest(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (GetConsumerGroupAssignmentRequest, error) {
		group, err := reader.readString()
		return GetConsumerGroupAssignmentRequest{Group: group}, err
	})
}

func encodeGetConsumerGroupAssignmentResponse(value any) ([]byte, error) {
	return encodeWith(value, func(writer *binaryWriter, resp GetConsumerGroupAssignmentResponse) error {
		if resp.Assignment == nil {
			writer.writeBool(false)
			return nil
		}
		writer.writeBool(true)
		return writeAssignment(writer, *resp.Assignment)
	})
}

func decodeGetConsumerGroupAssignmentResponse(data []byte, target any) error {
	return decodeWith(data, target, func(reader *binaryReader) (GetConsumerGroupAssignmentResponse, error) {
		ok, err := reader.readBool()
		if err != nil || !ok {
			return GetConsumerGroupAssignmentResponse{}, err
		}
		assignment, err := readAssignment(reader)
		return GetConsumerGroupAssignmentResponse{Assignment: &assignment}, err
	})
}

func writeAssignment(writer *binaryWriter, assignment ConsumerGroupAssignment) error {
	if err := writer.writeString(assignment.Group); err != nil {
		return err
	}
	writer.writeU64(assignment.Generation)
	count, err := checkedUint32(len(assignment.Assignments), "assignments")
	if err != nil {
		return err
	}
	writer.writeU32(count)
	for _, item := range assignment.Assignments {
		if err := writer.writeString(item.MemberID); err != nil {
			return err
		}
		if err := writer.writeString(item.Topic); err != nil {
			return err
		}
		writer.writeU32(item.Partition)
	}
	writer.writeU64(assignment.UpdatedAtMS)
	return nil
}

func readAssignment(reader *binaryReader) (ConsumerGroupAssignment, error) {
	group, err := reader.readString()
	if err != nil {
		return ConsumerGroupAssignment{}, err
	}
	assignment := ConsumerGroupAssignment{Group: group}
	if assignment.Generation, err = reader.readU64(); err != nil {
		return ConsumerGroupAssignment{}, err
	}
	if assignment.Assignments, err = readAssignmentItems(reader); err != nil {
		return ConsumerGroupAssignment{}, err
	}
	if assignment.UpdatedAtMS, err = reader.readU64(); err != nil {
		return ConsumerGroupAssignment{}, err
	}
	return assignment, nil
}

func readAssignmentItems(reader *binaryReader) ([]GroupPartitionAssignment, error) {
	count, err := reader.readU32()
	if err != nil {
		return nil, err
	}
	size, err := intFromUint32(count)
	if err != nil {
		return nil, err
	}
	items := newDecodedList[GroupPartitionAssignment](size)
	for range size {
		item, err := readAssignmentItem(reader)
		if err != nil {
			return nil, err
		}
		items.Add(item)
	}
	return items.Values(), nil
}

func readAssignmentItem(reader *binaryReader) (GroupPartitionAssignment, error) {
	memberID, err := reader.readString()
	if err != nil {
		return GroupPartitionAssignment{}, err
	}
	topic, err := reader.readString()
	if err != nil {
		return GroupPartitionAssignment{}, err
	}
	partition, err := reader.readU32()
	return GroupPartitionAssignment{MemberID: memberID, Topic: topic, Partition: partition}, err
}
