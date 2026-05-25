package broker

import (
	"cmp"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

type ClusterMetadata struct {
	NodeID      uint64              `json:"node_id"`
	ClusterName string              `json:"cluster_name"`
	RuntimeMode string              `json:"runtime_mode"`
	Raft        ClusterRaftMetadata `json:"raft"`
	Discovery   *DiscoveryHealth    `json:"discovery,omitempty"`
	DataShards  []DataShardHealth   `json:"data_shards,omitempty"`
}

type ClusterRaftMetadata struct {
	Engine             string                    `json:"engine"`
	BindAddr           string                    `json:"bind_addr"`
	AdvertiseAddr      string                    `json:"advertise_addr"`
	KnownNodes         int                       `json:"known_nodes"`
	Peers              []ClusterPeerMetadata     `json:"peers"`
	Health             *RaftHealth               `json:"health,omitempty"`
	LeaderDistribution ClusterLeaderDistribution `json:"leader_distribution"`
}

type ClusterPeerMetadata struct {
	NodeID uint64 `json:"node_id"`
	Addr   string `json:"addr"`
	Local  bool   `json:"local"`
}

type ClusterLeaderDistribution struct {
	GroupCount        int                    `json:"group_count"`
	ReadyGroups       int                    `json:"ready_groups"`
	Leaders           []ClusterLeaderSummary `json:"leaders"`
	LeaderlessGroups  []uint64               `json:"leaderless_groups,omitempty"`
	LocalLeaderGroups []uint64               `json:"local_leader_groups,omitempty"`
}

type ClusterLeaderSummary struct {
	NodeID     uint64 `json:"node_id"`
	GroupCount int    `json:"group_count"`
}

func (b *Broker) ClusterMetadata() ClusterMetadata {
	health := b.RuntimeHealth()
	return ClusterMetadata{
		NodeID:      b.cfg.Broker.NodeID,
		ClusterName: b.cfg.Broker.ClusterName,
		RuntimeMode: health.RuntimeMode,
		Raft: ClusterRaftMetadata{
			Engine:             "dragonboat",
			BindAddr:           b.cfg.Raft.BindAddr,
			AdvertiseAddr:      raftAdvertiseAddress(b.cfg),
			KnownNodes:         len(b.cfg.Raft.Cluster),
			Peers:              clusterPeerMetadata(b.cfg),
			Health:             health.Raft,
			LeaderDistribution: clusterLeaderDistribution(health.Raft),
		},
		Discovery:  health.Discovery,
		DataShards: health.DataShards,
	}
}

func clusterPeerMetadata(cfg Config) []ClusterPeerMetadata {
	peers := collectionlist.NewListWithCapacity[ClusterPeerMetadata](len(cfg.Raft.Cluster))
	for _, peer := range cfg.Raft.Cluster {
		peers.Add(ClusterPeerMetadata{
			NodeID: peer.NodeID,
			Addr:   peer.Addr,
			Local:  peer.NodeID == cfg.Broker.NodeID,
		})
	}
	return peers.Sort(func(left, right ClusterPeerMetadata) int {
		return cmp.Compare(left.NodeID, right.NodeID)
	}).Values()
}

func clusterLeaderDistribution(health *RaftHealth) ClusterLeaderDistribution {
	distribution := ClusterLeaderDistribution{}
	if health == nil {
		return distribution
	}
	leaderGroups := collectionlist.NewList[uint64]()
	leaderCounts := collectionmapping.NewMap[uint64, int]()
	localLeaderGroups := collectionlist.NewList[uint64]()
	for _, group := range health.Groups {
		distribution.GroupCount++
		if !group.Ready || group.LeaderID == 0 {
			leaderGroups.Add(group.GroupID)
			continue
		}
		distribution.ReadyGroups++
		leaderCounts.Set(group.LeaderID, leaderCounts.GetOrDefault(group.LeaderID, 0)+1)
		if group.LocalIsLeader {
			localLeaderGroups.Add(group.GroupID)
		}
	}
	distribution.LeaderlessGroups = leaderGroups.Sort(cmp.Compare[uint64]).Values()
	distribution.LocalLeaderGroups = localLeaderGroups.
		Sort(cmp.Compare[uint64]).
		Values()
	distribution.Leaders = clusterLeaderSummaries(leaderCounts)
	return distribution
}

func clusterLeaderSummaries(counts *collectionmapping.Map[uint64, int]) []ClusterLeaderSummary {
	leaders := collectionlist.NewList[ClusterLeaderSummary]()
	counts.Range(func(nodeID uint64, groupCount int) bool {
		leaders.Add(ClusterLeaderSummary{NodeID: nodeID, GroupCount: groupCount})
		return true
	})
	return leaders.Sort(func(left, right ClusterLeaderSummary) int {
		return cmp.Compare(left.NodeID, right.NodeID)
	}).Values()
}
