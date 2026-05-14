package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

type snapshotJSON struct {
	Topics            json.RawMessage `json:"topics"`
	Records           json.RawMessage `json:"records"`
	LogOffsets        json.RawMessage `json:"log_offsets"`
	Offsets           json.RawMessage `json:"offsets"`
	ConsumerPauses    json.RawMessage `json:"consumer_pauses"`
	Placements        json.RawMessage `json:"shard_placements"`
	Members           json.RawMessage `json:"members"`
	Assignments       json.RawMessage `json:"assignments"`
	Transactions      json.RawMessage `json:"transactions"`
	ProducerBatches   json.RawMessage `json:"producer_batches"`
	ACLPolicies       json.RawMessage `json:"acl_policies"`
	NextTransactionID uint64          `json:"next_transaction_id,omitempty"`
	BrokerState       *BrokerState    `json:"broker_state,omitempty"`
}

type snapshotCollections struct {
	topics          *collectionlist.List[TopicConfig]
	records         *collectionmapping.Map[string, []Record]
	logOffsets      *collectionmapping.Map[string, uint64]
	offsets         *collectionmapping.Map[string, uint64]
	consumerPauses  *collectionlist.List[ConsumerPauseState]
	placements      *collectionlist.List[ShardPlacement]
	members         *collectionlist.List[ConsumerGroupMember]
	assignments     *collectionlist.List[ConsumerGroupAssignment]
	transactions    *collectionlist.List[TransactionState]
	producerBatches *collectionlist.List[ProducerPublishedBatch]
	aclPolicies     *collectionlist.List[ACLPolicy]
}

func (s Snapshot) MarshalJSON() ([]byte, error) {
	wire, err := s.snapshotJSON()
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot json: %w", err)
	}
	return raw, nil
}

func (s Snapshot) snapshotJSON() (snapshotJSON, error) {
	wire := snapshotJSON{NextTransactionID: s.NextTransactionID, BrokerState: s.BrokerState}
	for _, item := range []struct {
		target *json.RawMessage
		value  json.Marshaler
		field  string
	}{
		{&wire.Topics, &s.Topics, "topics"},
		{&wire.Records, &s.Records, "records"},
		{&wire.LogOffsets, &s.LogOffsets, "log_offsets"},
		{&wire.Offsets, &s.Offsets, "offsets"},
		{&wire.ConsumerPauses, &s.ConsumerPauses, "consumer pauses"},
		{&wire.Placements, &s.Placements, "shard placements"},
		{&wire.Members, &s.Members, "members"},
		{&wire.Assignments, &s.Assignments, "assignments"},
		{&wire.Transactions, &s.Transactions, "transactions"},
		{&wire.ProducerBatches, &s.ProducerBatches, "producer batches"},
		{&wire.ACLPolicies, &s.ACLPolicies, "acl policies"},
	} {
		raw, err := marshalSnapshotCollection(item.value, item.field)
		if err != nil {
			return snapshotJSON{}, err
		}
		*item.target = raw
	}
	return wire, nil
}

func (s *Snapshot) UnmarshalJSON(data []byte) error {
	if s == nil {
		return errors.New("unmarshal snapshot json: nil receiver")
	}
	var wire snapshotJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("unmarshal snapshot json: %w", err)
	}
	collections, err := unmarshalSnapshotCollections(wire)
	if err != nil {
		return err
	}
	*s = Snapshot{
		Topics:            *collections.topics,
		Records:           *collections.records,
		LogOffsets:        *collections.logOffsets,
		Offsets:           *collections.offsets,
		ConsumerPauses:    *collections.consumerPauses,
		Placements:        *collections.placements,
		Members:           *collections.members,
		Assignments:       *collections.assignments,
		Transactions:      *collections.transactions,
		ProducerBatches:   *collections.producerBatches,
		ACLPolicies:       *collections.aclPolicies,
		NextTransactionID: wire.NextTransactionID,
		BrokerState:       wire.BrokerState,
	}
	return nil
}

func unmarshalSnapshotCollections(wire snapshotJSON) (snapshotCollections, error) {
	collections := snapshotCollections{
		topics:          collectionlist.NewList[TopicConfig](),
		records:         collectionmapping.NewMap[string, []Record](),
		logOffsets:      collectionmapping.NewMap[string, uint64](),
		offsets:         collectionmapping.NewMap[string, uint64](),
		consumerPauses:  collectionlist.NewList[ConsumerPauseState](),
		placements:      collectionlist.NewList[ShardPlacement](),
		members:         collectionlist.NewList[ConsumerGroupMember](),
		assignments:     collectionlist.NewList[ConsumerGroupAssignment](),
		transactions:    collectionlist.NewList[TransactionState](),
		producerBatches: collectionlist.NewList[ProducerPublishedBatch](),
		aclPolicies:     collectionlist.NewList[ACLPolicy](),
	}
	for _, item := range []struct {
		data   json.RawMessage
		target json.Unmarshaler
		field  string
	}{
		{wire.Topics, collections.topics, "topics"},
		{wire.Records, collections.records, "records"},
		{wire.LogOffsets, collections.logOffsets, "log_offsets"},
		{wire.Offsets, collections.offsets, "offsets"},
		{wire.ConsumerPauses, collections.consumerPauses, "consumer pauses"},
		{wire.Placements, collections.placements, "shard placements"},
		{wire.Members, collections.members, "members"},
		{wire.Assignments, collections.assignments, "assignments"},
		{wire.Transactions, collections.transactions, "transactions"},
		{wire.ProducerBatches, collections.producerBatches, "producer batches"},
		{wire.ACLPolicies, collections.aclPolicies, "acl policies"},
	} {
		if err := unmarshalSnapshotCollection(item.data, item.target, item.field); err != nil {
			return snapshotCollections{}, err
		}
	}
	return collections, nil
}

func marshalSnapshotCollection(value json.Marshaler, field string) (json.RawMessage, error) {
	raw, err := value.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot %s: %w", field, err)
	}
	return json.RawMessage(raw), nil
}

func unmarshalSnapshotCollection(data json.RawMessage, target json.Unmarshaler, field string) error {
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}
	if err := target.UnmarshalJSON(data); err != nil {
		return fmt.Errorf("unmarshal snapshot %s: %w", field, err)
	}
	return nil
}
