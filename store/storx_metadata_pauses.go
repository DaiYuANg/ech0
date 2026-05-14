package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) SaveConsumerPause(state ConsumerPauseState) error {
	if err := validateConsumerPauseState(state); err != nil {
		return err
	}
	return wrapExternal(s.consumerPauses.Put(context.Background(), consumerPauseKey(state.Consumer, state.TopicPartition()), state), "save consumer pause")
}

func (s *StorxMetadataStore) LoadConsumerPause(consumer string, topicPartition TopicPartition) (*ConsumerPauseState, error) {
	state, ok, err := s.consumerPauses.Get(context.Background(), consumerPauseKey(consumer, topicPartition))
	if err != nil {
		return nil, wrapExternal(err, "load consumer pause")
	}
	if !ok {
		var absent *ConsumerPauseState
		return absent, nil
	}
	return &state, nil
}

func (s *StorxMetadataStore) ListConsumerPauses() ([]ConsumerPauseState, error) {
	states := collectionlist.NewList[ConsumerPauseState]()
	err := s.consumerPauses.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerPauseState]) error {
		states.Add(entry.Value)
		return nil
	})
	if err != nil {
		return nil, wrapExternal(err, "list consumer pauses")
	}
	return sortConsumerPauses(states.Values()), nil
}

func (s *StorxMetadataStore) DeleteConsumerPause(consumer string, topicPartition TopicPartition) error {
	return wrapExternal(s.consumerPauses.Delete(context.Background(), consumerPauseKey(consumer, topicPartition)), "delete consumer pause")
}
