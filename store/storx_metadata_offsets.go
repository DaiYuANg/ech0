package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) SaveConsumerOffsetState(state ConsumerOffsetState) error {
	if err := validateConsumerOffsetState(state); err != nil {
		return err
	}
	key := offsetKey(state.Consumer, state.TopicPartition())
	if err := s.offsets.Put(context.Background(), key, state.NextOffset); err != nil {
		return wrapExternal(err, "save consumer offset")
	}
	return wrapExternal(s.offsetStates.Put(context.Background(), key, state), "save consumer offset state")
}

func (s *StorxMetadataStore) LoadConsumerOffsetState(consumer string, topicPartition TopicPartition) (*ConsumerOffsetState, error) {
	key := offsetKey(consumer, topicPartition)
	state, ok, err := s.offsetStates.Get(context.Background(), key)
	if err != nil {
		return nil, wrapExternal(err, "load consumer offset state")
	}
	if ok {
		return &state, nil
	}
	offset, ok, err := s.offsets.Get(context.Background(), key)
	if err != nil {
		return nil, wrapExternal(err, "load consumer offset")
	}
	if !ok {
		var absent *ConsumerOffsetState
		return absent, nil
	}
	state = ConsumerOffsetState{Consumer: consumer, Topic: topicPartition.Topic, Partition: topicPartition.Partition, NextOffset: offset}
	return &state, nil
}

func (s *StorxMetadataStore) ListConsumerOffsetStates() ([]ConsumerOffsetState, error) {
	states := collectionlist.NewList[ConsumerOffsetState]()
	err := s.offsetStates.Walk(context.Background(), func(entry bboltx.Entry[string, ConsumerOffsetState]) error {
		states.Add(entry.Value)
		return nil
	})
	if err != nil {
		return nil, wrapExternal(err, "list consumer offset states")
	}
	return sortConsumerOffsetStates(states.Values()), nil
}
