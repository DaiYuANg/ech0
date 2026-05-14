package broker

import "github.com/lyonbrown4d/ech0/store"

func (b *Broker) validateConsumerGroupLease(group, memberID string, generation uint64, tp store.TopicPartition) error {
	if group == "" || memberID == "" {
		return brokerStoreError(store.CodeInvalidArgument, "group and member_id are required")
	}
	if generation == 0 {
		return brokerStoreError(store.CodeInvalidArgument, "consumer group %s generation is required", group)
	}
	if _, err := b.loadUnexpiredGroupMember(group, memberID, store.NowMS()); err != nil {
		return err
	}
	assignment, err := b.currentConsumerGroupAssignment(group, generation)
	if err != nil {
		return err
	}
	return validateConsumerGroupPartitionOwner(group, memberID, generation, assignment, tp)
}

func (b *Broker) loadUnexpiredGroupMember(group, memberID string, nowMS uint64) (*store.ConsumerGroupMember, error) {
	member, err := b.meta.LoadGroupMember(group, memberID)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group member")
	}
	if member == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "group member %s/%s not found", group, memberID)
	}
	if member.ExpiredAt(nowMS) {
		return nil, brokerStoreError(store.CodeInvalidArgument, "group member %s/%s lease expired", group, memberID)
	}
	return member, nil
}

func (b *Broker) currentConsumerGroupAssignment(group string, generation uint64) (*store.ConsumerGroupAssignment, error) {
	assignment, err := b.meta.LoadGroupAssignment(group)
	if err != nil {
		return nil, wrapBrokerStore(err, "load group assignment")
	}
	if assignment == nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "consumer group %s has no active assignment", group)
	}
	if assignment.Generation != generation {
		return nil, brokerStoreError(store.CodeInvalidArgument, "stale consumer group generation for %s: got %d want %d", group, generation, assignment.Generation)
	}
	return assignment, nil
}

func validateConsumerGroupPartitionOwner(
	group string,
	memberID string,
	generation uint64,
	assignment *store.ConsumerGroupAssignment,
	tp store.TopicPartition,
) error {
	owner, ok := assignmentOwnerTable(assignment.Assignments).Get(tp.Topic, tp.Partition)
	if !ok {
		return brokerStoreError(store.CodeInvalidArgument, "consumer group %s has no owner for %s/%d in generation %d", group, tp.Topic, tp.Partition, generation)
	}
	if owner != memberID {
		return brokerStoreError(store.CodeInvalidArgument, "member %s does not own %s/%d in group %s generation %d", memberID, tp.Topic, tp.Partition, group, generation)
	}
	return nil
}
