package broker

import (
	"cmp"
	"context"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
)

type ClusterMembershipResult struct {
	Operation string                         `json:"operation"`
	NodeID    uint64                         `json:"node_id"`
	Addr      string                         `json:"addr,omitempty"`
	Groups    []ClusterMembershipGroupResult `json:"groups"`
}

type ClusterMembershipGroupResult struct {
	GroupID uint64 `json:"group_id"`
	Changed bool   `json:"changed"`
	Error   string `json:"error,omitempty"`
}

type ClusterLeadershipTransferResult struct {
	GroupID    uint64 `json:"group_id"`
	TargetNode uint64 `json:"target_node"`
	Requested  bool   `json:"requested"`
	Error      string `json:"error,omitempty"`
}

type ClusterLeadershipBalanceResult struct {
	Transfers []ClusterLeadershipTransferResult `json:"transfers"`
}

func (b *Broker) JoinClusterNode(ctx context.Context, nodeID uint64, addr string) (ClusterMembershipResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAdmin, clusterResource(identity)); err != nil {
		return ClusterMembershipResult{}, err
	}
	addr = strings.TrimSpace(addr)
	if nodeID == 0 || addr == "" {
		return ClusterMembershipResult{}, brokerStoreError(store.CodeInvalidArgument, "cluster node id and addr are required")
	}
	return b.changeClusterMembership(ctx, "join", nodeID, addr)
}

func (b *Broker) LeaveClusterNode(ctx context.Context, nodeID uint64) (ClusterMembershipResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAdmin, clusterResource(identity)); err != nil {
		return ClusterMembershipResult{}, err
	}
	if nodeID == 0 {
		return ClusterMembershipResult{}, brokerStoreError(store.CodeInvalidArgument, "cluster node id is required")
	}
	return b.changeClusterMembership(ctx, "leave", nodeID, "")
}

func (b *Broker) TransferClusterLeadership(ctx context.Context, groupID, targetNodeID uint64) (ClusterLeadershipTransferResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAdmin, clusterResource(identity)); err != nil {
		return ClusterLeadershipTransferResult{}, err
	}
	if groupID == 0 || targetNodeID == 0 {
		return ClusterLeadershipTransferResult{}, brokerStoreError(store.CodeInvalidArgument, "group id and target node id are required")
	}
	node := b.raftNode()
	if node == nil {
		return ClusterLeadershipTransferResult{}, brokerStoreError(store.CodeUnavailable, "raft runtime is not started")
	}
	return node.transferLeadership(groupID, targetNodeID), nil
}

func (b *Broker) BalanceClusterLeadership(ctx context.Context) (ClusterLeadershipBalanceResult, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAdmin, clusterResource(identity)); err != nil {
		return ClusterLeadershipBalanceResult{}, err
	}
	node := b.raftNode()
	if node == nil {
		return ClusterLeadershipBalanceResult{}, brokerStoreError(store.CodeUnavailable, "raft runtime is not started")
	}
	return ClusterLeadershipBalanceResult{Transfers: node.balanceLeadership()}, nil
}

func (b *Broker) changeClusterMembership(ctx context.Context, operation string, nodeID uint64, addr string) (ClusterMembershipResult, error) {
	node := b.raftNode()
	if node == nil {
		return ClusterMembershipResult{}, brokerStoreError(store.CodeUnavailable, "raft runtime is not started")
	}
	result := ClusterMembershipResult{Operation: operation, NodeID: nodeID, Addr: addr}
	for _, groupID := range node.groups {
		result.Groups = append(result.Groups, node.changeGroupMembership(ctx, operation, groupID, nodeID, addr))
	}
	return result, nil
}

func (b *Broker) raftNode() *raftNode {
	b.raftMu.RLock()
	defer b.raftMu.RUnlock()
	return b.raft
}

func (n *raftNode) changeGroupMembership(ctx context.Context, operation string, groupID, nodeID uint64, addr string) ClusterMembershipGroupResult {
	result := ClusterMembershipGroupResult{GroupID: groupID}
	changeCtx, cancel := contextWithRaftApplyTimeout(ctx, n.cfg.Raft.ApplyTimeoutMS)
	defer cancel()
	membership, err := n.nodeHost.SyncGetShardMembership(changeCtx, groupID)
	if err != nil {
		result.Error = errorMessage(wrapBroker("dragonboat_membership_lookup_failed", err, "lookup raft group membership"))
		return result
	}
	if operation == "join" {
		result.Changed, result.Error = n.addGroupReplica(changeCtx, groupID, nodeID, addr, membership.ConfigChangeID)
		return result
	}
	result.Changed, result.Error = n.deleteGroupReplica(changeCtx, groupID, nodeID, membership.ConfigChangeID)
	return result
}

func (n *raftNode) addGroupReplica(ctx context.Context, groupID, nodeID uint64, addr string, configChangeID uint64) (bool, string) {
	membership, err := n.nodeHost.SyncGetShardMembership(ctx, groupID)
	if err == nil {
		if _, ok := membership.Nodes[nodeID]; ok {
			return false, ""
		}
	}
	err = n.nodeHost.SyncRequestAddReplica(ctx, groupID, nodeID, addr, configChangeID)
	return err == nil, errorMessage(wrapClusterControlError(err, "add raft replica"))
}

func (n *raftNode) deleteGroupReplica(ctx context.Context, groupID, nodeID, configChangeID uint64) (bool, string) {
	membership, err := n.nodeHost.SyncGetShardMembership(ctx, groupID)
	if err == nil {
		if _, ok := membership.Nodes[nodeID]; !ok {
			return false, ""
		}
	}
	err = n.nodeHost.SyncRequestDeleteReplica(ctx, groupID, nodeID, configChangeID)
	return err == nil, errorMessage(wrapClusterControlError(err, "delete raft replica"))
}

func (n *raftNode) transferLeadership(groupID, targetNodeID uint64) ClusterLeadershipTransferResult {
	result := ClusterLeadershipTransferResult{GroupID: groupID, TargetNode: targetNodeID}
	err := n.nodeHost.RequestLeaderTransfer(groupID, targetNodeID)
	result.Requested = err == nil
	result.Error = errorMessage(wrapClusterControlError(err, "transfer raft leadership"))
	return result
}

func (n *raftNode) balanceLeadership() []ClusterLeadershipTransferResult {
	health := n.Health()
	leaders := leaderLoadFromHealth(health)
	transfers := collectionlist.NewList[ClusterLeadershipTransferResult]()
	for _, group := range sortedRaftGroups(health.Groups) {
		target, ok := leastLoadedLeaderTarget(leaders, group.LeaderID)
		if !ok || target == group.LeaderID {
			continue
		}
		transfer := n.transferLeadership(group.GroupID, target)
		transfers.Add(transfer)
		if transfer.Requested {
			leaders.Set(group.LeaderID, max(leaders.GetOrDefault(group.LeaderID, 0)-1, 0))
			leaders.Set(target, leaders.GetOrDefault(target, 0)+1)
		}
	}
	return transfers.Values()
}

func leaderLoadFromHealth(health *RaftHealth) *collectionmapping.Map[uint64, int] {
	load := collectionmapping.NewMap[uint64, int]()
	if health == nil {
		return load
	}
	for _, group := range health.Groups {
		if !group.Ready || group.LeaderID == 0 {
			continue
		}
		load.Set(group.LeaderID, load.GetOrDefault(group.LeaderID, 0)+1)
	}
	return load
}

func leastLoadedLeaderTarget(load *collectionmapping.Map[uint64, int], current uint64) (uint64, bool) {
	if load == nil {
		return 0, false
	}
	found := false
	target := uint64(0)
	targetLoad := 0
	load.Range(func(nodeID uint64, count int) bool {
		if !found || count < targetLoad || (count == targetLoad && nodeID < target) {
			target = nodeID
			targetLoad = count
			found = true
		}
		return true
	})
	if !found {
		return 0, false
	}
	if current == 0 || load.GetOrDefault(current, 0)-targetLoad < 2 {
		return 0, false
	}
	return target, true
}

func sortedRaftGroups(groups []RaftGroupHealth) []RaftGroupHealth {
	return collectionlist.NewList(groups...).
		Sort(func(left, right RaftGroupHealth) int {
			return cmp.Compare(left.GroupID, right.GroupID)
		}).
		Values()
}

func wrapClusterControlError(err error, message string) error {
	if err == nil {
		return nil
	}
	return wrapBroker("cluster_control_failed", err, "%s", message)
}
