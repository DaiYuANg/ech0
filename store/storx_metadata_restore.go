package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) Restore(snapshot Snapshot) error {
	if err := s.clearMetadataBuckets(); err != nil {
		return err
	}
	if err := s.restoreMetadataTopics(snapshot.Topics); err != nil {
		return err
	}
	if err := s.restoreMetadataOffsets(snapshot); err != nil {
		return err
	}
	if err := s.restoreMetadataMembers(snapshot.Members); err != nil {
		return err
	}
	if err := s.restoreMetadataAssignments(snapshot.Assignments); err != nil {
		return err
	}
	return s.restoreMetadataBrokerState(snapshot.BrokerState)
}

func (s *StorxMetadataStore) clearMetadataBuckets() error {
	for _, bucket := range []interface{ Clear(context.Context) error }{
		bucketClearer[string, TopicConfig]{s.topics},
		bucketClearer[string, uint64]{s.offsets},
		bucketClearer[string, ConsumerGroupMember]{s.members.Repository().Bucket()},
		bucketClearer[string, ConsumerGroupAssignment]{s.assignments},
		bucketClearer[string, BrokerState]{s.brokerState},
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
	return wrapExternal(s.offsets.PutMany(context.Background(), entries.Values()...), "restore consumer offsets")
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
