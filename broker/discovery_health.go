package broker

import "github.com/lyonbrown4d/ech0/discovery"

type DiscoveryHealth struct {
	Enabled         bool                  `json:"enabled"`
	Provider        DiscoveryProvider     `json:"provider,omitempty"`
	KnownNodes      int                   `json:"known_nodes"`
	BootstrapExpect int                   `json:"bootstrap_expect,omitempty"`
	Nodes           []DiscoveryNodeHealth `json:"nodes,omitempty"`
}

type DiscoveryNodeHealth struct {
	ID          uint64               `json:"id"`
	Name        string               `json:"name,omitempty"`
	ClusterName string               `json:"cluster_name,omitempty"`
	RaftAddr    string               `json:"raft_addr,omitempty"`
	Status      discovery.NodeStatus `json:"status,omitempty"`
}

func (b *Broker) discoveryHealth() *DiscoveryHealth {
	if b == nil || !b.cfg.Discovery.Enabled {
		return nil
	}
	health := &DiscoveryHealth{
		Enabled:         true,
		Provider:        b.cfg.Discovery.Provider,
		BootstrapExpect: b.discoveryBootstrapExpect(),
	}
	if b.discovery == nil {
		return health
	}
	nodes := b.discovery.Nodes()
	health.KnownNodes = len(nodes)
	health.Nodes = make([]DiscoveryNodeHealth, 0, len(nodes))
	for _, node := range nodes {
		health.Nodes = append(health.Nodes, DiscoveryNodeHealth{
			ID:          node.ID,
			Name:        node.Name,
			ClusterName: node.ClusterName,
			RaftAddr:    node.RaftAddr,
			Status:      node.Status,
		})
	}
	return health
}
