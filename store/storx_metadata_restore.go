package store

import "context"

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
		bucketClearer[string, ConsumerGroupMember]{s.members},
		bucketClearer[string, ConsumerGroupAssignment]{s.assignments},
		bucketClearer[string, BrokerState]{s.brokerState},
	} {
		if err := bucket.Clear(context.Background()); err != nil {
			return wrapExternal(err, "clear metadata bucket")
		}
	}
	return nil
}

func (s *StorxMetadataStore) restoreMetadataTopics(topics []TopicConfig) error {
	for i := range topics {
		if err := s.SaveTopicConfig(topics[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorxMetadataStore) restoreMetadataOffsets(snapshot Snapshot) error {
	var putErr error
	snapshot.Offsets.Range(func(key string, offset uint64) bool {
		if err := s.offsets.Put(context.Background(), key, offset); err != nil {
			putErr = wrapExternal(err, "restore consumer offset")
			return false
		}
		return true
	})
	return putErr
}

func (s *StorxMetadataStore) restoreMetadataMembers(members []ConsumerGroupMember) error {
	for _, member := range members {
		if err := s.SaveGroupMember(member); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorxMetadataStore) restoreMetadataAssignments(assignments []ConsumerGroupAssignment) error {
	for _, assignment := range assignments {
		if err := s.SaveGroupAssignment(assignment); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorxMetadataStore) restoreMetadataBrokerState(state *BrokerState) error {
	if state == nil {
		return nil
	}
	return s.SaveBrokerState(*state)
}
