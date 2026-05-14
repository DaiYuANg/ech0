package memberlist

import (
	"encoding/json"

	hashimemberlist "github.com/hashicorp/memberlist"
	"github.com/lyonbrown4d/ech0/discovery"
)

const metadataVersion = 1

type nodeMetadata struct {
	Version        int               `json:"v"`
	ID             uint64            `json:"id"`
	ClusterName    string            `json:"cluster_name"`
	RaftAddr       string            `json:"raft_addr"`
	BrokerAddr     string            `json:"broker_addr,omitempty"`
	AdminAddr      string            `json:"admin_addr,omitempty"`
	DataShardCount uint32            `json:"data_shard_count,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

func nodeMetadataFromNode(node discovery.Node) nodeMetadata {
	return nodeMetadata{
		Version:        metadataVersion,
		ID:             node.ID,
		ClusterName:    node.ClusterName,
		RaftAddr:       node.RaftAddr,
		BrokerAddr:     node.BrokerAddr,
		AdminAddr:      node.AdminAddr,
		DataShardCount: node.DataShardCount,
		Tags:           node.Tags,
	}
}

func nodeFromMember(member *hashimemberlist.Node) discovery.Node {
	if member == nil {
		return discovery.Node{}
	}
	var metadata nodeMetadata
	if err := json.Unmarshal(member.Meta, &metadata); err != nil {
		return discovery.Node{Name: member.Name, Status: nodeStatus(member.State)}
	}
	return discovery.Node{
		ID:             metadata.ID,
		Name:           member.Name,
		ClusterName:    metadata.ClusterName,
		RaftAddr:       metadata.RaftAddr,
		BrokerAddr:     metadata.BrokerAddr,
		AdminAddr:      metadata.AdminAddr,
		DataShardCount: metadata.DataShardCount,
		Status:         nodeStatus(member.State),
		Tags:           metadata.Tags,
	}
}

func nodeStatus(status hashimemberlist.NodeStateType) discovery.NodeStatus {
	switch status {
	case hashimemberlist.StateAlive:
		return discovery.NodeStatusAlive
	case hashimemberlist.StateSuspect:
		return discovery.NodeStatusSuspect
	case hashimemberlist.StateDead:
		return discovery.NodeStatusDead
	case hashimemberlist.StateLeft:
		return discovery.NodeStatusLeft
	default:
		return discovery.NodeStatusUnknown
	}
}
