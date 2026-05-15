package broker

import (
	"context"

	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) waitForRaftRead(ctx context.Context, topic string, partition uint32) error {
	if b == nil || b.cfg.Raft.ReadPolicy == RaftReadLocal {
		return nil
	}
	node := b.currentRaftNode()
	if node == nil {
		return brokerStoreError(store.CodeUnavailable, "raft runtime is not started")
	}
	groupID, err := b.readGroupID(topic, partition)
	if err != nil {
		return err
	}
	if b.cfg.Raft.ReadPolicy == RaftReadLeader {
		localLeader, err := node.localLeaderForGroup(groupID)
		if err != nil {
			return err
		}
		if !localLeader {
			return brokerStoreError(store.CodeNotLeader, "node is not raft leader for read group %d", groupID)
		}
	}
	return node.ReadBarrier(ctx, groupID)
}

func (b *Broker) readGroupID(topic string, partition uint32) (uint64, error) {
	if !dataRaftEnabled(b.cfg) {
		return raftMetadataGroupID, nil
	}
	if b.shards == nil {
		return dataShardRaftGroupID(store.ShardID(partition)), nil
	}
	placement, err := b.shards.Resolve(store.NewTopicPartition(topic, partition))
	if err != nil {
		return 0, err
	}
	return dataShardRaftGroupID(placement.ShardID), nil
}

func (n *raftNode) ReadBarrier(ctx context.Context, groupID uint64) error {
	if err := n.validateApply(ctx); err != nil {
		return err
	}
	readCtx, cancel := contextWithRaftApplyTimeout(ctx, n.cfg.Raft.ApplyTimeoutMS)
	defer cancel()
	if _, err := n.nodeHost.SyncRead(readCtx, groupID, nil); err != nil {
		return raftApplyError(err)
	}
	return nil
}

func (n *raftNode) localLeaderForGroup(groupID uint64) (bool, error) {
	if n == nil || n.nodeHost == nil {
		return false, brokerStoreError(store.CodeUnavailable, "raft runtime is not started")
	}
	leaderID, _, ready, err := n.nodeHost.GetLeaderID(groupID)
	if err != nil {
		return false, wrapBroker("raft_leader_lookup_failed", err, "lookup raft read group %d leader", groupID)
	}
	return ready && leaderID == n.cfg.Broker.NodeID, nil
}
