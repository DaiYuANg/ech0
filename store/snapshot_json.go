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
	Topics      json.RawMessage `json:"topics"`
	Records     json.RawMessage `json:"records"`
	LogOffsets  json.RawMessage `json:"log_offsets"`
	Offsets     json.RawMessage `json:"offsets"`
	Placements  json.RawMessage `json:"shard_placements"`
	Members     json.RawMessage `json:"members"`
	Assignments json.RawMessage `json:"assignments"`
	BrokerState *BrokerState    `json:"broker_state,omitempty"`
}

func (s Snapshot) MarshalJSON() ([]byte, error) {
	topics, err := marshalSnapshotCollection(&s.Topics, "topics")
	if err != nil {
		return nil, err
	}
	records, err := marshalSnapshotCollection(&s.Records, "records")
	if err != nil {
		return nil, err
	}
	logOffsets, err := marshalSnapshotCollection(&s.LogOffsets, "log_offsets")
	if err != nil {
		return nil, err
	}
	offsets, err := marshalSnapshotCollection(&s.Offsets, "offsets")
	if err != nil {
		return nil, err
	}
	placements, err := marshalSnapshotCollection(&s.Placements, "shard placements")
	if err != nil {
		return nil, err
	}
	members, err := marshalSnapshotCollection(&s.Members, "members")
	if err != nil {
		return nil, err
	}
	assignments, err := marshalSnapshotCollection(&s.Assignments, "assignments")
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(snapshotJSON{
		Topics:      topics,
		Records:     records,
		LogOffsets:  logOffsets,
		Offsets:     offsets,
		Placements:  placements,
		Members:     members,
		Assignments: assignments,
		BrokerState: s.BrokerState,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot json: %w", err)
	}
	return raw, nil
}

func (s *Snapshot) UnmarshalJSON(data []byte) error {
	if s == nil {
		return errors.New("unmarshal snapshot json: nil receiver")
	}
	var wire snapshotJSON
	if err := json.Unmarshal(data, &wire); err != nil {
		return fmt.Errorf("unmarshal snapshot json: %w", err)
	}
	topics := collectionlist.NewList[TopicConfig]()
	records := collectionmapping.NewMap[string, []Record]()
	logOffsets := collectionmapping.NewMap[string, uint64]()
	offsets := collectionmapping.NewMap[string, uint64]()
	placements := collectionlist.NewList[ShardPlacement]()
	members := collectionlist.NewList[ConsumerGroupMember]()
	assignments := collectionlist.NewList[ConsumerGroupAssignment]()
	if err := unmarshalSnapshotCollection(wire.Topics, topics, "topics"); err != nil {
		return err
	}
	if err := unmarshalSnapshotCollection(wire.Records, records, "records"); err != nil {
		return err
	}
	if err := unmarshalSnapshotCollection(wire.LogOffsets, logOffsets, "log_offsets"); err != nil {
		return err
	}
	if err := unmarshalSnapshotCollection(wire.Offsets, offsets, "offsets"); err != nil {
		return err
	}
	if err := unmarshalSnapshotCollection(wire.Placements, placements, "shard placements"); err != nil {
		return err
	}
	if err := unmarshalSnapshotCollection(wire.Members, members, "members"); err != nil {
		return err
	}
	if err := unmarshalSnapshotCollection(wire.Assignments, assignments, "assignments"); err != nil {
		return err
	}
	*s = Snapshot{
		Topics:      *topics,
		Records:     *records,
		LogOffsets:  *logOffsets,
		Offsets:     *offsets,
		Placements:  *placements,
		Members:     *members,
		Assignments: *assignments,
		BrokerState: wire.BrokerState,
	}
	return nil
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
