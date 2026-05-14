package broker

import "github.com/lyonbrown4d/ech0/store"

func (b *Broker) validateConsumerGroupLease(group, memberID string, generation uint64, tp store.TopicPartition) error {
	if group == "" || memberID == "" {
		return brokerStoreError(store.CodeInvalidArgument, "group and member_id are required")
	}
	if generation == 0 {
		return brokerStoreError(store.CodeInvalidArgument, "consumer group %s generation is required", group)
	}
	assignment, err := b.meta.LoadGroupAssignment(group)
	if err != nil {
		return wrapBrokerStore(err, "load group assignment")
	}
	if assignment == nil {
		return brokerStoreError(store.CodeInvalidArgument, "consumer group %s has no active assignment", group)
	}
	if assignment.Generation != generation {
		return brokerStoreError(store.CodeInvalidArgument, "stale consumer group generation for %s: got %d want %d", group, generation, assignment.Generation)
	}
	owner, ok := assignmentOwnerTable(assignment.Assignments).Get(tp.Topic, tp.Partition)
	if !ok {
		return brokerStoreError(store.CodeInvalidArgument, "consumer group %s has no owner for %s/%d in generation %d", group, tp.Topic, tp.Partition, generation)
	}
	if owner != memberID {
		return brokerStoreError(store.CodeInvalidArgument, "member %s does not own %s/%d in group %s generation %d", memberID, tp.Topic, tp.Partition, group, generation)
	}
	return nil
}
