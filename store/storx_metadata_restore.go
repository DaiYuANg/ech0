package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) Restore(snapshot Snapshot) error {
	for _, step := range s.restoreMetadataSteps(snapshot) {
		if err := step(); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorxMetadataStore) restoreMetadataSteps(snapshot Snapshot) []func() error {
	return []func() error{
		s.clearMetadataBuckets,
		func() error { return s.restoreMetadataTopics(snapshot.Topics) },
		func() error { return s.restoreMetadataOffsets(snapshot) },
		func() error { return s.restoreMetadataConsumerPauses(snapshot.ConsumerPauses) },
		func() error { return s.restoreMetadataShardPlacements(snapshot.Placements) },
		func() error { return s.restoreMetadataMembers(snapshot.Members) },
		func() error { return s.restoreMetadataAssignments(snapshot.Assignments) },
		func() error { return s.restoreMetadataTransactions(snapshot) },
		func() error { return s.restoreMetadataProducerBatches(snapshot.ProducerBatches) },
		func() error { return s.restoreMetadataACLPolicies(snapshot.ACLPolicies) },
		func() error { return s.restoreMetadataBrokerState(snapshot.BrokerState) },
	}
}

func (s *StorxMetadataStore) clearMetadataBuckets() error {
	for _, bucket := range []interface{ Clear(context.Context) error }{
		bucketClearer[string, TopicConfig]{s.topics},
		bucketClearer[string, uint64]{s.offsets},
		bucketClearer[string, ConsumerOffsetState]{s.offsetStates},
		bucketClearer[string, ConsumerPauseState]{s.consumerPauses},
		bucketClearer[string, ConsumerGroupMember]{s.members.Repository().Bucket()},
		bucketClearer[string, ConsumerGroupAssignment]{s.assignments},
		bucketClearer[string, BrokerState]{s.brokerState},
		bucketClearer[string, ShardPlacement]{s.placements},
		bucketClearer[string, TransactionState]{s.transactions},
		bucketClearer[string, uint64]{s.transactionCounters},
		bucketClearer[string, ProducerPublishedBatch]{s.producerBatches},
		bucketClearer[string, ACLPolicy]{s.aclPolicies},
	} {
		if err := bucket.Clear(context.Background()); err != nil {
			return wrapExternal(err, "clear metadata bucket")
		}
	}
	if err := s.members.RebuildIndexes(context.Background()); err != nil {
		return wrapExternal(err, "clear group member indexes")
	}
	return nil
}

func (s *StorxMetadataStore) restoreMetadataTopics(topics collectionlist.List[TopicConfig]) error {
	var resultErr error
	topics.Range(func(_ int, topic TopicConfig) bool {
		if err := s.SaveTopicConfig(topic); err != nil {
			resultErr = err
			return false
		}
		return true
	})
	return resultErr
}

func (s *StorxMetadataStore) restoreMetadataOffsets(snapshot Snapshot) error {
	entries := collectionlist.NewListWithCapacity[bboltx.Entry[string, uint64]](snapshot.Offsets.Len())
	snapshot.Offsets.Range(func(key string, offset uint64) bool {
		entries.Add(bboltx.Entry[string, uint64]{Key: key, Value: offset})
		return true
	})
	if err := s.offsets.PutMany(context.Background(), entries.Values()...); err != nil {
		return wrapExternal(err, "restore consumer offsets")
	}
	return s.restoreMetadataOffsetStates(snapshot)
}

func (s *StorxMetadataStore) restoreMetadataOffsetStates(snapshot Snapshot) error {
	states := restoreMemoryOffsetStates(snapshot.OffsetStates, snapshot.Offsets).Values()
	entries := collectionlist.NewListWithCapacity[bboltx.Entry[string, ConsumerOffsetState]](len(states))
	for _, state := range states {
		if validateConsumerOffsetState(state) == nil {
			entries.Add(bboltx.Entry[string, ConsumerOffsetState]{Key: offsetKey(state.Consumer, state.TopicPartition()), Value: state})
		}
	}
	return wrapExternal(s.offsetStates.PutMany(context.Background(), entries.Values()...), "restore consumer offset states")
}

func (s *StorxMetadataStore) restoreMetadataConsumerPauses(pauses collectionlist.List[ConsumerPauseState]) error {
	var resultErr error
	pauses.Range(func(_ int, state ConsumerPauseState) bool {
		if !state.Paused {
			return true
		}
		if err := s.SaveConsumerPause(state); err != nil {
			resultErr = err
			return false
		}
		return true
	})
	return resultErr
}

func (s *StorxMetadataStore) restoreMetadataShardPlacements(placements collectionlist.List[ShardPlacement]) error {
	entries := collectionlist.NewListWithCapacity[bboltx.Entry[string, ShardPlacement]](placements.Len())
	placements.Range(func(_ int, placement ShardPlacement) bool {
		if validateShardPlacement(placement) == nil {
			entries.Add(bboltx.Entry[string, ShardPlacement]{
				Key:   partitionKey(placement.TopicPartition()),
				Value: cloneShardPlacement(placement),
			})
		}
		return true
	})
	return wrapExternal(s.placements.PutMany(context.Background(), entries.Values()...), "restore shard placements")
}

func (s *StorxMetadataStore) restoreMetadataMembers(members collectionlist.List[ConsumerGroupMember]) error {
	var resultErr error
	members.Range(func(_ int, member ConsumerGroupMember) bool {
		if err := s.SaveGroupMember(member); err != nil {
			resultErr = err
			return false
		}
		return true
	})
	return resultErr
}

func (s *StorxMetadataStore) restoreMetadataAssignments(assignments collectionlist.List[ConsumerGroupAssignment]) error {
	var resultErr error
	assignments.Range(func(_ int, assignment ConsumerGroupAssignment) bool {
		if err := s.SaveGroupAssignment(assignment); err != nil {
			resultErr = err
			return false
		}
		return true
	})
	return resultErr
}

func (s *StorxMetadataStore) restoreMetadataBrokerState(state *BrokerState) error {
	if state == nil {
		return nil
	}
	return s.SaveBrokerState(*state)
}

func (s *StorxMetadataStore) restoreMetadataACLPolicies(policies collectionlist.List[ACLPolicy]) error {
	entries := collectionlist.NewListWithCapacity[bboltx.Entry[string, ACLPolicy]](policies.Len())
	policies.Range(func(_ int, policy ACLPolicy) bool {
		if validateACLPolicy(policy) == nil {
			entries.Add(bboltx.Entry[string, ACLPolicy]{Key: policy.PolicyID, Value: cloneACLPolicy(policy)})
		}
		return true
	})
	return wrapExternal(s.aclPolicies.PutMany(context.Background(), entries.Values()...), "restore acl policies")
}

func (s *StorxMetadataStore) restoreMetadataProducerBatches(batches collectionlist.List[ProducerPublishedBatch]) error {
	entries := collectionlist.NewListWithCapacity[bboltx.Entry[string, ProducerPublishedBatch]](batches.Len())
	batches.Range(func(_ int, batch ProducerPublishedBatch) bool {
		if validateProducerBatch(batch) == nil {
			entries.Add(bboltx.Entry[string, ProducerPublishedBatch]{
				Key:   producerBatchKey(batch),
				Value: cloneProducerPublishedBatch(batch),
			})
		}
		return true
	})
	return wrapExternal(s.producerBatches.PutMany(context.Background(), entries.Values()...), "restore producer batches")
}
