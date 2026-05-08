package broker

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	hashiraft "github.com/hashicorp/raft"
)

type raftNode struct {
	cfg       Config
	broker    *Broker
	raft      *hashiraft.Raft
	transport *hashiraft.NetworkTransport
	store     *raftBoltStore
}

type raftResources struct {
	transport *hashiraft.NetworkTransport
	snapshots hashiraft.SnapshotStore
	store     *raftBoltStore
}

func startRaft(ctx context.Context, b *Broker) (*raftNode, error) {
	if err := validateRaftStores(b); err != nil {
		return nil, err
	}
	resources, err := openRaftResources(ctx, b)
	if err != nil {
		return nil, err
	}
	conf := raftRuntimeConfig(b.cfg)
	if bootstrapErr := bootstrapRaftIfNeeded(conf, resources, b.cfg.Raft.Cluster); bootstrapErr != nil {
		return nil, bootstrapErr
	}
	r, err := hashiraft.NewRaft(conf, &brokerFSM{broker: b}, resources.store, resources.store, resources.snapshots, resources.transport)
	if err != nil {
		return nil, resources.closeWith(wrapBroker("raft_create_failed", err, "create raft runtime"))
	}
	return &raftNode{cfg: b.cfg, broker: b, raft: r, transport: resources.transport, store: resources.store}, nil
}

func validateRaftStores(b *Broker) error {
	if _, ok := b.meta.(store.Snapshotter); !ok {
		return brokerStoreError(store.CodeInvalidArgument, "raft mode requires metadata store to implement store.Snapshotter")
	}
	if _, ok := b.log.(store.Snapshotter); !ok {
		return brokerStoreError(store.CodeInvalidArgument, "raft mode requires log store to implement store.Snapshotter")
	}
	return nil
}

func openRaftResources(ctx context.Context, b *Broker) (*raftResources, error) {
	raftDir := b.cfg.RaftDir()
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, wrapBroker("raft_directory_create_failed", err, "create raft directory")
	}
	advertise, err := raftAdvertiseAddress(b.cfg)
	if err != nil {
		return nil, err
	}
	transport, err := hashiraft.NewTCPTransport(b.cfg.Raft.BindAddr, advertise, 3, 10*time.Second, io.Discard)
	if err != nil {
		return nil, wrapBroker("raft_transport_create_failed", err, "create raft transport")
	}
	resources := &raftResources{transport: transport}
	if err := resources.openStores(ctx, raftDir, b.metrics); err != nil {
		return nil, err
	}
	return resources, nil
}

func raftAdvertiseAddress(cfg Config) (net.Addr, error) {
	addr := cfg.Raft.BindAddr
	for _, peer := range cfg.Raft.Cluster {
		if peer.NodeID == cfg.Broker.NodeID && peer.Addr != "" {
			addr = peer.Addr
			break
		}
	}
	advertise, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, wrapBroker("raft_advertise_resolve_failed", err, "resolve raft advertise address %s", addr)
	}
	return advertise, nil
}

func (r *raftResources) openStores(ctx context.Context, raftDir string, metrics *MetricsRuntime) error {
	snapshots, err := hashiraft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), 2, io.Discard)
	if err != nil {
		return r.closeWith(wrapBroker("raft_snapshot_store_create_failed", err, "create raft snapshot store"))
	}
	r.snapshots = snapshots
	bolt, err := openRaftBoltStore(ctx, filepath.Join(raftDir, "raft.db"), metrics)
	if err != nil {
		return r.closeWith(wrapBroker("raft_bolt_store_create_failed", err, "create raft bolt store"))
	}
	r.store = bolt
	return nil
}

func raftRuntimeConfig(cfg Config) *hashiraft.Config {
	conf := hashiraft.DefaultConfig()
	conf.LocalID = hashiraft.ServerID(strconv.FormatUint(cfg.Broker.NodeID, 10))
	conf.HeartbeatTimeout = durationFromMillis(cfg.Raft.HeartbeatIntervalMS)
	conf.ElectionTimeout = durationFromMillis(cfg.Raft.ElectionTimeoutMaxMS)
	conf.LeaderLeaseTimeout = conf.HeartbeatTimeout
	conf.CommitTimeout = durationFromMillis(cfg.Raft.CommitTimeoutMS)
	return conf
}

func bootstrapRaftIfNeeded(conf *hashiraft.Config, resources *raftResources, peers []RaftPeerConfig) error {
	hasState, err := hashiraft.HasExistingState(resources.store, resources.store, resources.snapshots)
	if err != nil {
		return resources.closeWith(wrapBroker("raft_existing_state_check_failed", err, "check raft existing state"))
	}
	if hasState {
		return nil
	}
	if err := hashiraft.BootstrapCluster(conf, resources.store, resources.store, resources.snapshots, resources.transport, raftConfiguration(peers)); err != nil {
		return resources.closeWith(wrapBroker("raft_bootstrap_failed", err, "bootstrap raft cluster"))
	}
	return nil
}

func raftConfiguration(peers []RaftPeerConfig) hashiraft.Configuration {
	servers := collectionlist.NewListWithCapacity[hashiraft.Server](len(peers))
	for _, peer := range peers {
		servers.Add(hashiraft.Server{
			ID:      hashiraft.ServerID(strconv.FormatUint(peer.NodeID, 10)),
			Address: hashiraft.ServerAddress(peer.Addr),
		})
	}
	return hashiraft.Configuration{Servers: servers.Values()}
}

func (r *raftResources) closeWith(err error) error {
	return errors.Join(err, r.close())
}

func (r *raftResources) close() error {
	if r == nil {
		return nil
	}
	var err error
	if r.store != nil {
		err = errors.Join(err, r.store.Close())
	}
	if r.transport != nil {
		err = errors.Join(err, r.transport.Close())
	}
	return err
}

func (n *raftNode) Apply(ctx context.Context, commandType string, payload any) (any, error) {
	if err := n.validateApply(ctx); err != nil {
		return nil, err
	}
	totalStart := time.Now()
	var resultErr error
	defer func() {
		n.recordRaftStage(ctx, commandType, "total", totalStart, resultErr)
	}()
	encodeStart := time.Now()
	encoded, err := encodeRaftCommand(commandType, payload)
	n.recordRaftStage(ctx, commandType, "encode", encodeStart, err)
	if err != nil {
		resultErr = err
		return nil, err
	}
	applyStart := time.Now()
	future := n.raft.Apply(encoded, durationFromMillis(n.cfg.Raft.ApplyTimeoutMS))
	if err := future.Error(); err != nil {
		wrapped := raftApplyError(err)
		n.recordRaftStage(ctx, commandType, "apply_wait", applyStart, wrapped)
		resultErr = wrapped
		return nil, wrapped
	}
	n.recordRaftStage(ctx, commandType, "apply_wait", applyStart, nil)
	responseStart := time.Now()
	value, responseErr := raftApplyResponse(future.Response())
	n.recordRaftStage(ctx, commandType, "response", responseStart, responseErr)
	resultErr = responseErr
	return value, responseErr
}

func (n *raftNode) recordRaftStage(ctx context.Context, commandType, stage string, start time.Time, err error) {
	if n == nil || n.broker == nil || n.broker.metrics == nil {
		return
	}
	n.broker.metrics.RecordRaftStage(ctx, commandType, stage, time.Since(start), err)
}

func (n *raftNode) validateApply(ctx context.Context) error {
	if n == nil || n.raft == nil {
		return brokerStoreError(store.CodeUnavailable, "raft runtime is not started")
	}
	if err := ctx.Err(); err != nil {
		return wrapBroker("raft_apply_context_done", err, "apply raft command")
	}
	return nil
}

func encodeRaftCommand(commandType string, payload any) ([]byte, error) {
	if encoded, ok, err := encodeBinaryRaftCommand(commandType, payload); ok || err != nil {
		if err != nil {
			return nil, wrapBroker("raft_binary_command_encode_failed", err, "encode binary raft command")
		}
		return encoded, nil
	}
	raw, err := marshalJSON(payload)
	if err != nil {
		return nil, wrapBroker("raft_payload_encode_failed", err, "encode raft payload")
	}
	encoded, err := marshalJSON(raftCommand{Type: commandType, Payload: raw})
	if err != nil {
		return nil, wrapBroker("raft_command_encode_failed", err, "encode raft command")
	}
	return encoded, nil
}

func raftApplyError(err error) error {
	if errors.Is(err, hashiraft.ErrNotLeader) || errors.Is(err, hashiraft.ErrLeadershipLost) {
		return brokerStoreError(store.CodeNotLeader, "node is not raft leader")
	}
	return wrapBroker("raft_apply_failed", err, "apply raft command")
}

func raftApplyResponse(response any) (any, error) {
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
	parsedLeader, err := strconv.ParseUint(string(leaderID), 10, 64)
	if err != nil {
		parsedLeader = 0
	}
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
