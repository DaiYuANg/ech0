package broker

import (
	"context"
	"errors"
	"os"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	raftMetadataGroupID uint64 = 1
	raftDataGroupBaseID uint64 = 10_000
	dragonboatRTTMS     uint64 = 10
)

type raftNode struct {
	cfg      Config
	broker   *Broker
	nodeHost *dragonboat.NodeHost
	groups   []uint64
}

type raftApplyWireResult struct {
	Value jsonRawMessage `json:"value,omitempty"`
	Error string         `json:"error,omitempty"`
}

func startRaft(ctx context.Context, b *Broker) (*raftNode, error) {
	if err := validateRaftStores(b); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(b.cfg.DragonboatDir(), 0o750); err != nil {
		return nil, wrapBroker("dragonboat_directory_create_failed", err, "create dragonboat directory")
	}
	nh, err := dragonboat.NewNodeHost(dragonboatNodeHostConfig(b.cfg))
	if err != nil {
		return nil, wrapBroker("dragonboat_nodehost_create_failed", err, "create dragonboat nodehost")
	}
	node := &raftNode{
		cfg:      b.cfg,
		broker:   b,
		nodeHost: nh,
		groups:   raftGroupIDs(b.cfg),
	}
	if err := node.startGroups(ctx); err != nil {
		return nil, node.closeWith(err)
	}
	if singleReplicaRaftCluster(b.cfg) {
		if err := node.waitForLocalLeaders(ctx); err != nil {
			return nil, node.closeWith(err)
		}
	}
	return node, nil
}

func validateRaftStores(b *Broker) error {
	if _, ok := b.meta.(store.Snapshotter); !ok {
		return brokerStoreError(store.CodeInvalidArgument, "dragonboat runtime requires metadata store to implement store.Snapshotter")
	}
	if b.queue == nil {
		return brokerStoreError(store.CodeInvalidArgument, "dragonboat runtime requires message runtime to implement store.Snapshotter")
	}
	return nil
}

func (n *raftNode) startGroups(ctx context.Context) error {
	for _, groupID := range n.groups {
		if err := ctx.Err(); err != nil {
			return wrapBroker("dragonboat_start_context_done", err, "start dragonboat raft groups")
		}
		if err := n.startGroup(groupID); err != nil {
			return err
		}
	}
	return nil
}

func (n *raftNode) startGroup(groupID uint64) error {
	join := false
	initialMembers := dragonboatInitialMembers(n.cfg)
	if n.nodeHost.HasNodeInfo(groupID, n.cfg.Broker.NodeID) {
		initialMembers = nil
	}
	rc := dragonboatGroupConfig(n.cfg, groupID)
	if err := n.nodeHost.StartReplica(initialMembers, join, n.newStateMachine, rc); err != nil {
		return wrapBroker("dragonboat_replica_start_failed", err, "start dragonboat replica for group %d", groupID)
	}
	return nil
}

func (n *raftNode) waitForLocalLeaders(ctx context.Context) error {
	waitCtx, cancel := contextWithRaftApplyTimeout(ctx, max(n.cfg.Raft.ApplyTimeoutMS, uint64(5_000)))
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if n.allGroupsHaveLocalLeader() {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return wrapBroker("dragonboat_local_leader_wait_failed", waitCtx.Err(), "wait for local dragonboat leaders")
		case <-ticker.C:
		}
	}
}

func (n *raftNode) allGroupsHaveLocalLeader() bool {
	for _, groupID := range n.groups {
		leaderID, _, ready, err := n.nodeHost.GetLeaderID(groupID)
		if err != nil || !ready || leaderID != n.cfg.Broker.NodeID {
			return false
		}
	}
	return true
}

func (n *raftNode) Apply(ctx context.Context, commandType string, payload any) (any, error) {
	return n.ApplyGroup(ctx, raftMetadataGroupID, commandType, payload)
}

func (n *raftNode) ApplyGroup(ctx context.Context, groupID uint64, commandType string, payload any) (any, error) {
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
	applyCtx, cancel := contextWithRaftApplyTimeout(ctx, n.cfg.Raft.ApplyTimeoutMS)
	defer cancel()
	applyStart := time.Now()
	result, err := n.nodeHost.SyncPropose(applyCtx, n.nodeHost.GetNoOPSession(groupID), encoded)
	if err != nil {
		wrapped := raftApplyError(err)
		n.recordRaftStage(ctx, commandType, "apply_wait", applyStart, wrapped)
		resultErr = wrapped
		return nil, wrapped
	}
	n.recordRaftStage(ctx, commandType, "apply_wait", applyStart, nil)
	responseStart := time.Now()
	value, responseErr := decodeRaftApplyResult(result.Data)
	n.recordRaftStage(ctx, commandType, "response", responseStart, responseErr)
	resultErr = responseErr
	return value, responseErr
}

func contextWithRaftApplyTimeout(ctx context.Context, timeoutMS uint64) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, durationFromMillis(timeoutMS))
}

func (n *raftNode) recordRaftStage(ctx context.Context, commandType, stage string, start time.Time, err error) {
	if n == nil || n.broker == nil || n.broker.metrics == nil {
		return
	}
	n.broker.metrics.RecordRaftStage(ctx, commandType, stage, time.Since(start), err)
}

func (n *raftNode) validateApply(ctx context.Context) error {
	if n == nil || n.nodeHost == nil {
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

func encodeRaftApplyResult(value any, applyErr error) ([]byte, error) {
	result := raftApplyWireResult{Error: errorMessage(applyErr)}
	if applyErr != nil || value == nil {
		return marshalJSON(result)
	}
	raw, err := marshalJSON(value)
	if err != nil {
		result.Error = errorMessage(wrapBroker("raft_result_encode_failed", err, "encode raft result"))
		return marshalJSON(result)
	}
	result.Value = raw
	return marshalJSON(result)
}

func decodeRaftApplyResult(data []byte) (any, error) {
	var result raftApplyWireResult
	if err := unmarshalJSON(data, &result); err != nil {
		return nil, wrapBroker("raft_result_decode_failed", err, "decode raft result")
	}
	if result.Error != "" {
		return nil, errorFromMessage(result.Error)
	}
	if len(result.Value) == 0 {
		return struct{}{}, nil
	}
	return result.Value, nil
}

func raftApplyError(err error) error {
	if errors.Is(err, dragonboat.ErrShardNotReady) || errors.Is(err, dragonboat.ErrShardNotInitialized) {
		return brokerStoreError(store.CodeNotLeader, "node is not raft leader")
	}
	if errors.Is(err, dragonboat.ErrTimeout) || errors.Is(err, dragonboat.ErrSystemBusy) {
		return brokerStoreError(store.CodeUnavailable, "raft apply unavailable: %v", err)
	}
	return wrapBroker("raft_apply_failed", err, "apply raft command")
}

func (n *raftNode) Health() *RaftHealth {
	if n == nil {
		return &RaftHealth{Engine: "dragonboat"}
	}
	health := &RaftHealth{
		NodeID:     n.cfg.Broker.NodeID,
		KnownNodes: len(n.cfg.Raft.Cluster),
		Engine:     "dragonboat",
		Groups:     make([]RaftGroupHealth, 0, len(n.groups)),
	}
	if n.nodeHost == nil {
		return health
	}
	for _, groupID := range n.groups {
		leaderID, _, ready, err := n.nodeHost.GetLeaderID(groupID)
		if err != nil {
			continue
		}
		if groupID == raftMetadataGroupID {
			health.LeaderID = leaderID
			health.LocalIsLeader = ready && leaderID == n.cfg.Broker.NodeID
		}
		health.Groups = append(health.Groups, RaftGroupHealth{
			GroupID:       groupID,
			LeaderID:      leaderID,
			LocalIsLeader: ready && leaderID == n.cfg.Broker.NodeID,
			Ready:         ready,
		})
	}
	return health
}

func (n *raftNode) Close() (err error) {
	if n == nil || n.nodeHost == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = errors.Join(err, brokerStoreError(store.CodeUnavailable, "close dragonboat nodehost panic: %v", recovered))
		}
	}()
	n.nodeHost.Close()
	n.nodeHost = nil
	return err
}

func (n *raftNode) closeWith(err error) error {
	return errors.Join(err, n.Close())
}
