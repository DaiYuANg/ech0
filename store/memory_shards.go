package store

import collectionlist "github.com/arcgolabs/collectionx/list"

func (s *MemoryStore) SaveShardPlacement(placement ShardPlacement) error {
	if err := validateShardPlacement(placement); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.placements.Set(placement.TopicPartition(), placement)
	return nil
}

func (s *MemoryStore) LoadShardPlacement(topicPartition TopicPartition) (*ShardPlacement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	placement, ok := s.placements.Get(topicPartition)
	if !ok {
		var absent *ShardPlacement
		return absent, nil
	}
	return &placement, nil
}

func (s *MemoryStore) ListShardPlacements() ([]ShardPlacement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := collectionlist.NewListWithCapacity[ShardPlacement](s.placements.Len())
	s.placements.Range(func(_ TopicPartition, placement ShardPlacement) bool {
		out.Add(placement)
		return true
	})
	return sortShardPlacements(out.Values()), nil
}
