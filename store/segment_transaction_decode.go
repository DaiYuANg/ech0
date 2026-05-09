package store

func (d *segmentRecordDecoder) readTransaction() (TransactionRecordMetadata, bool, error) {
	present, err := d.readByte()
	if err != nil {
		return TransactionRecordMetadata{}, false, err
	}
	if present == 0 {
		return TransactionRecordMetadata{}, false, nil
	}
	txID, err := d.readU64("transaction tx_id")
	if err != nil {
		return TransactionRecordMetadata{}, false, err
	}
	producerID, err := d.readU64("transaction producer_id")
	if err != nil {
		return TransactionRecordMetadata{}, false, err
	}
	producerEpoch, err := d.readU64("transaction producer_epoch")
	if err != nil {
		return TransactionRecordMetadata{}, false, err
	}
	sequence, err := d.readU64("transaction sequence")
	if err != nil {
		return TransactionRecordMetadata{}, false, err
	}
	control, err := d.readTransactionControl()
	if err != nil {
		return TransactionRecordMetadata{}, false, err
	}
	return TransactionRecordMetadata{
		TxID:          txID,
		ProducerID:    producerID,
		ProducerEpoch: producerEpoch,
		Sequence:      sequence,
		ControlType:   control,
	}, true, nil
}

func (d *segmentRecordDecoder) readTransactionControl() (TransactionControlType, error) {
	raw, err := d.readByte()
	if err != nil {
		return "", err
	}
	switch raw {
	case 0:
		return TransactionControlNone, nil
	case 1:
		return TransactionControlCommit, nil
	case 2:
		return TransactionControlAbort, nil
	default:
		return "", E(CodeCodec, "unknown segment transaction control type %d", raw)
	}
}
