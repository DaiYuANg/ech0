package broker

import "github.com/DaiYuANg/ech0/store"

func (b *Broker) appendTransactionControlMarkers(
	state *store.TransactionState,
	controlType store.TransactionControlType,
) error {
	if controlType == store.TransactionControlNone {
		return brokerStoreError(store.CodeInvalidArgument, "transaction control marker type is required")
	}
	identity := transactionIdentityFromState(*state)
	sequence := state.NextSequence
	for _, topicPartition := range state.Partitions {
		if err := b.appendTransactionControlMarker(identity, topicPartition, sequence, controlType); err != nil {
			return err
		}
		nextSequence, err := incrementTransactionSequence(sequence)
		if err != nil {
			return err
		}
		sequence = nextSequence
	}
	state.NextSequence = sequence
	return nil
}

func (b *Broker) appendTransactionControlMarker(
	identity TransactionIdentity,
	topicPartition store.TopicPartition,
	sequence uint64,
	controlType store.TransactionControlType,
) error {
	marker := store.RecordAppend{
		Transaction: transactionRecordMetadata(identity, sequence, controlType),
	}
	_, err := b.queue.PublishRecord(topicPartition.Topic, topicPartition.Partition, marker)
	if err != nil {
		return wrapBroker("tx_control_marker_append_failed", err, "append transaction control marker")
	}
	return nil
}

func incrementTransactionSequence(sequence uint64) (uint64, error) {
	if sequence == ^uint64(0) {
		return 0, brokerStoreError(store.CodeInvalidArgument, "transaction sequence overflows uint64")
	}
	return sequence + 1, nil
}
