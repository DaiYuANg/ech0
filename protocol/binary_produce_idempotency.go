package protocol

func writeProduceIdempotency(writer *binaryWriter, id *ProduceIdempotency) {
	if id == nil {
		writer.writeBool(false)
		return
	}
	writer.writeBool(true)
	writer.writeU64(id.ProducerID)
	writer.writeU64(id.ProducerEpoch)
	writer.writeU64(id.BaseSequence)
}

func readProduceIdempotency(reader *binaryReader) (*ProduceIdempotency, error) {
	ok, err := reader.readBool()
	if err != nil || !ok {
		return nil, err
	}
	id := ProduceIdempotency{}
	if id.ProducerID, err = reader.readU64(); err != nil {
		return nil, err
	}
	if id.ProducerEpoch, err = reader.readU64(); err != nil {
		return nil, err
	}
	if id.BaseSequence, err = reader.readU64(); err != nil {
		return nil, err
	}
	return &id, nil
}
