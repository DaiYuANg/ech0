package broker

import (
	"path/filepath"

	"github.com/DaiYuANg/ech0/store"
	"github.com/cespare/xxhash/v2"
	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
)

const dragonboatQueueMultiplier uint64 = 8

func dragonboatNodeHostConfig(cfg Config) config.NodeHostConfig {
	advertise := raftAdvertiseAddress(cfg)
	return config.NodeHostConfig{
		DeploymentID:     xxhash.Sum64String(cfg.Broker.ClusterName),
		NodeHostDir:      cfg.DragonboatDir(),
		WALDir:           filepath.Join(cfg.DragonboatDir(), "wal"),
		RTTMillisecond:   dragonboatRTTMS,
		RaftAddress:      advertise,
		ListenAddress:    cfg.Raft.BindAddr,
		MaxSendQueueSize: dragonboatPayloadQueueSize(cfg),
	}
}

func raftAdvertiseAddress(cfg Config) string {
	for _, peer := range cfg.Raft.Cluster {
		if peer.NodeID == cfg.Broker.NodeID && peer.Addr != "" {
			return peer.Addr
		}
	}
	return cfg.Raft.BindAddr
}

func singleReplicaRaftCluster(cfg Config) bool {
	return len(cfg.Raft.Cluster) == 1 && cfg.Raft.Cluster[0].NodeID == cfg.Broker.NodeID
}

func dataRaftEnabled(cfg Config) bool {
	return len(cfg.Raft.Cluster) > 1
}

func dragonboatInitialMembers(cfg Config) map[uint64]dragonboat.Target {
	members := make(map[uint64]dragonboat.Target, len(cfg.Raft.Cluster))
	for _, peer := range cfg.Raft.Cluster {
		if peer.NodeID == 0 || peer.Addr == "" {
			continue
		}
		members[peer.NodeID] = peer.Addr
	}
	return members
}

func dragonboatGroupConfig(cfg Config, groupID uint64) config.Config {
	return config.Config{
		ReplicaID:          cfg.Broker.NodeID,
		ShardID:            groupID,
		CheckQuorum:        true,
		PreVote:            true,
		HeartbeatRTT:       raftRTTTicks(cfg.Raft.HeartbeatIntervalMS),
		ElectionRTT:        raftElectionRTTTicks(cfg.Raft.HeartbeatIntervalMS, cfg.Raft.ElectionTimeoutMaxMS),
		SnapshotEntries:    10_000,
		CompactionOverhead: 1_000,
		MaxInMemLogSize:    max(dragonboatPayloadQueueSize(cfg), uint64(64*1024*1024)),
		WaitReady:          true,
	}
}

func dragonboatPayloadQueueSize(cfg Config) uint64 {
	bytes := nonNegativeIntToUint64(cfg.Broker.MaxBatchPayloadBytes)
	if bytes > ^uint64(0)/dragonboatQueueMultiplier {
		return ^uint64(0)
	}
	return bytes * dragonboatQueueMultiplier
}

func nonNegativeIntToUint64(value int) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func raftRTTTicks(milliseconds uint64) uint64 {
	ticks := ceilDiv(milliseconds, dragonboatRTTMS)
	if ticks == 0 {
		return 1
	}
	return ticks
}

func raftElectionRTTTicks(heartbeatMS, electionMS uint64) uint64 {
	heartbeatTicks := raftRTTTicks(heartbeatMS)
	electionTicks := raftRTTTicks(electionMS)
	minElectionTicks := heartbeatTicks*2 + 1
	if electionTicks < minElectionTicks {
		return minElectionTicks
	}
	return electionTicks
}

func ceilDiv(value, divisor uint64) uint64 {
	if divisor == 0 {
		return 0
	}
	return (value + divisor - 1) / divisor
}

func raftGroupIDs(cfg Config) []uint64 {
	count := cfg.Broker.DataShardCount
	if count == 0 {
		count = 1
	}
	groups := make([]uint64, 0, int(count)+1)
	groups = append(groups, raftMetadataGroupID)
	if !dataRaftEnabled(cfg) {
		return groups
	}
	for shardIndex := range count {
		groups = append(groups, dataShardRaftGroupID(store.ShardID(shardIndex)))
	}
	return groups
}

func dataShardRaftGroupID(shardID store.ShardID) uint64 {
	return raftDataGroupBaseID + uint64(shardID)
}
