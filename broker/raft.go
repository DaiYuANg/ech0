package broker

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/DaiYuANg/ech0/store"
	hashiraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

const (
	raftCommandCreateTopic    = "create_topic"
	raftCommandProduce        = "produce"
	raftCommandProduceBatch   = "produce_batch"
	raftCommandCommitOffset   = "commit_offset"
	raftCommandDirectSend     = "direct_send"
	raftCommandDirectAck      = "direct_ack"
	raftCommandJoinGroup      = "join_group"
	raftCommandHeartbeatGroup = "heartbeat_group"
	raftCommandRebalanceGroup = "rebalance_group"
)

type raftCommand struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type raftNode struct {
	cfg       Config
	broker    *Broker
	raft      *hashiraft.Raft
	transport *hashiraft.NetworkTransport
	store     *raftboltdb.BoltStore
}

func startRaft(ctx context.Context, b *Broker) (*raftNode, error) {
	_ = ctx
	snapshotter, ok := b.meta.(store.Snapshotter)
	if !ok {
		return nil, store.E(store.CodeInvalidArgument, "raft mode requires metadata store to implement store.Snapshotter")
	}
	if _, ok := b.log.(store.Snapshotter); !ok {
		return nil, store.E(store.CodeInvalidArgument, "raft mode requires log store to implement store.Snapshotter")
	}
	_ = snapshotter

	raftDir := b.cfg.RaftDir()
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, err
	}

	conf := hashiraft.DefaultConfig()
	conf.LocalID = hashiraft.ServerID(strconv.FormatUint(b.cfg.Broker.NodeID, 10))
	conf.HeartbeatTimeout = time.Duration(b.cfg.Raft.HeartbeatIntervalMS) * time.Millisecond
	conf.ElectionTimeout = time.Duration(b.cfg.Raft.ElectionTimeoutMaxMS) * time.Millisecond
	conf.LeaderLeaseTimeout = conf.HeartbeatTimeout
	conf.CommitTimeout = 50 * time.Millisecond

	transport, err := hashiraft.NewTCPTransport(b.cfg.Raft.BindAddr, nil, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, err
	}
	snapshots, err := hashiraft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), 2, io.Discard)
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		_ = transport.Close()
		return nil, err
	}

	fsm := &brokerFSM{broker: b}
	hasState, err := hashiraft.HasExistingState(bolt, bolt, snapshots)
	if err != nil {
		_ = bolt.Close()
		_ = transport.Close()
		return nil, err
	}
	if !hasState {
		configuration := hashiraft.Configuration{Servers: make([]hashiraft.Server, 0, len(b.cfg.Raft.Cluster))}
		for _, peer := range b.cfg.Raft.Cluster {
			configuration.Servers = append(configuration.Servers, hashiraft.Server{
				ID:      hashiraft.ServerID(strconv.FormatUint(peer.NodeID, 10)),
				Address: hashiraft.ServerAddress(peer.Addr),
			})
		}
		if err := hashiraft.BootstrapCluster(conf, bolt, bolt, snapshots, transport, configuration); err != nil {
			_ = bolt.Close()
			_ = transport.Close()
			return nil, err
		}
	}

	r, err := hashiraft.NewRaft(conf, fsm, bolt, bolt, snapshots, transport)
	if err != nil {
		_ = bolt.Close()
		_ = transport.Close()
		return nil, err
	}
	return &raftNode{cfg: b.cfg, broker: b, raft: r, transport: transport, store: bolt}, nil
}

func (n *raftNode) Apply(ctx context.Context, commandType string, payload any) (any, error) {
	if n == nil || n.raft == nil {
		return nil, store.E(store.CodeUnavailable, "raft runtime is not started")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(raftCommand{Type: commandType, Payload: raw})
	if err != nil {
		return nil, err
	}
	timeout := time.Duration(n.cfg.Raft.ApplyTimeoutMS) * time.Millisecond
	future := n.raft.Apply(encoded, timeout)
	if err := future.Error(); err != nil {
		if err == hashiraft.ErrNotLeader || err == hashiraft.ErrLeadershipLost {
			return nil, store.E(store.CodeNotLeader, "node is not raft leader")
		}
		return nil, err
	}
	response := future.Response()
	if err, ok := response.(error); ok {
		return nil, err
	}
	if applied, ok := response.(applyResult); ok {
		return applied.Value, applied.Err
	}
	return response, nil
}

func (n *raftNode) Health() *RaftHealth {
	if n == nil {
		return &RaftHealth{}
	}
	if n.raft == nil {
		return &RaftHealth{NodeID: n.cfg.Broker.NodeID, KnownNodes: len(n.cfg.Raft.Cluster)}
	}
	_, leaderID := n.raft.LeaderWithID()
	parsedLeader, _ := strconv.ParseUint(string(leaderID), 10, 64)
	return &RaftHealth{
		NodeID:        n.cfg.Broker.NodeID,
		KnownNodes:    len(n.cfg.Raft.Cluster),
		LeaderID:      parsedLeader,
		LocalIsLeader: parsedLeader == n.cfg.Broker.NodeID,
	}
}

func (n *raftNode) Close() error {
	if n == nil {
		return nil
	}
	var err error
	if n.raft != nil {
		if shutdownErr := n.raft.Shutdown().Error(); shutdownErr != nil {
			err = shutdownErr
		}
	}
	if n.transport != nil {
		if closeErr := n.transport.Close(); err == nil {
			err = closeErr
		}
	}
	if n.store != nil {
		if closeErr := n.store.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

type applyResult struct {
	Value any
	Err   error
}

type brokerFSM struct {
	broker *Broker
}

func (f *brokerFSM) Apply(log *hashiraft.Log) interface{} {
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

func (f *brokerFSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	var snapshot store.Snapshot
	if err := json.NewDecoder(reader).Decode(&snapshot); err != nil {
		return err
	}
	return f.broker.restore(snapshot)
}

type brokerSnapshot struct {
	snapshot store.Snapshot
}

func (s *brokerSnapshot) Persist(sink hashiraft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.snapshot)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *brokerSnapshot) Release() {}
