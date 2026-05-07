package broker

import (
	"errors"
	"io"

	"github.com/DaiYuANg/ech0/store"
	hashiraft "github.com/hashicorp/raft"
)

type applyResult struct {
	Value any
	Err   error
}

type brokerFSM struct {
	broker *Broker
}

func (f *brokerFSM) Apply(log *hashiraft.Log) any {
	value, err := f.broker.applyRaftCommand(log.Data)
	return applyResult{Value: value, Err: err}
}

func (f *brokerFSM) Snapshot() (hashiraft.FSMSnapshot, error) {
	snapshot, err := f.broker.snapshot()
	if err != nil {
		return nil, err
	}
	return &brokerSnapshot{snapshot: snapshot}, nil
}

func (f *brokerFSM) Restore(reader io.ReadCloser) (err error) {
	defer func() {
		err = errors.Join(err, reader.Close())
	}()
	var snapshot store.Snapshot
	if err := newJSONDecoder(reader).Decode(&snapshot); err != nil {
		return wrapBroker("raft_snapshot_decode_failed", err, "decode raft snapshot")
	}
	return f.broker.restore(snapshot)
}

type brokerSnapshot struct {
	snapshot store.Snapshot
}

func (s *brokerSnapshot) Persist(sink hashiraft.SnapshotSink) error {
	err := newJSONEncoder(sink).Encode(s.snapshot)
	if err != nil {
		err = errors.Join(err, sink.Cancel())
		return err
	}
	return wrapBroker("raft_snapshot_close_failed", sink.Close(), "close raft snapshot sink")
}

func (s *brokerSnapshot) Release() {}
