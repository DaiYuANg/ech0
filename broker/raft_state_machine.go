package broker

import (
	"io"

	sm "github.com/lni/dragonboat/v4/statemachine"
	"github.com/lyonbrown4d/ech0/store"
)

type raftStateMachine struct {
	broker *Broker
}

func (n *raftNode) newStateMachine(uint64, uint64) sm.IStateMachine {
	return &raftStateMachine{broker: n.broker}
}

func (m *raftStateMachine) Update(entry sm.Entry) (sm.Result, error) {
	value, applyErr := m.broker.applyRaftCommand(entry.Cmd)
	data, err := encodeRaftApplyResult(value, applyErr)
	if err != nil {
		return sm.Result{}, err
	}
	return sm.Result{Data: data}, nil
}

func (m *raftStateMachine) Lookup(any) (any, error) {
	return struct{}{}, nil
}

func (m *raftStateMachine) SaveSnapshot(writer io.Writer, _ sm.ISnapshotFileCollection, done <-chan struct{}) error {
	snapshot, err := m.broker.snapshot()
	if err != nil {
		return err
	}
	if err := doneErr(done); err != nil {
		return err
	}
	if err := newJSONEncoder(writer).Encode(snapshot); err != nil {
		return wrapBroker("raft_snapshot_encode_failed", err, "encode raft snapshot")
	}
	return nil
}

func (m *raftStateMachine) RecoverFromSnapshot(reader io.Reader, _ []sm.SnapshotFile, done <-chan struct{}) error {
	if err := doneErr(done); err != nil {
		return err
	}
	var snapshot store.Snapshot
	if err := newJSONDecoder(reader).Decode(&snapshot); err != nil {
		return wrapBroker("raft_snapshot_decode_failed", err, "decode raft snapshot")
	}
	if err := doneErr(done); err != nil {
		return err
	}
	return m.broker.restore(snapshot)
}

func (m *raftStateMachine) Close() error {
	return nil
}

func doneErr(done <-chan struct{}) error {
	select {
	case <-done:
		return sm.ErrSnapshotStopped
	default:
		return nil
	}
}
