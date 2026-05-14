package broker

import (
	"cmp"
	"context"
	"fmt"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/discovery"
	"github.com/lyonbrown4d/ech0/store"
)

const discoveryWaitInterval = 100 * time.Millisecond

func discoveryNodeFromConfig(cfg Config) discovery.Node {
	return discovery.Node{
		ID:             cfg.Broker.NodeID,
		Name:           fmt.Sprintf("%s-%d", cfg.Broker.ClusterName, cfg.Broker.NodeID),
		ClusterName:    cfg.Broker.ClusterName,
		RaftAddr:       raftAdvertiseAddress(cfg),
		BrokerAddr:     cfg.Broker.BindAddr,
		AdminAddr:      cfg.Admin.BindAddr,
		DataShardCount: cfg.Broker.DataShardCount,
		Status:         discovery.NodeStatusAlive,
	}
}

func discoveryNodesFromRaftConfig(cfg Config) []discovery.Node {
	nodes := collectionlist.NewListWithCapacity[discovery.Node](len(cfg.Raft.Cluster))
	for _, peer := range cfg.Raft.Cluster {
		if peer.NodeID == 0 || peer.Addr == "" {
			continue
		}
		nodes.Add(discovery.Node{
			ID:             peer.NodeID,
			Name:           fmt.Sprintf("%s-%d", cfg.Broker.ClusterName, peer.NodeID),
			ClusterName:    cfg.Broker.ClusterName,
			RaftAddr:       peer.Addr,
			DataShardCount: cfg.Broker.DataShardCount,
			Status:         discovery.NodeStatusAlive,
		})
	}
	if nodes.Len() == 0 {
		nodes.Add(discoveryNodeFromConfig(cfg))
	}
	return nodes.Values()
}

func (b *Broker) startDiscovery(ctx context.Context) error {
	if b == nil || b.discovery == nil || !b.cfg.Discovery.Enabled {
		return nil
	}
	if err := b.discovery.Start(ctx); err != nil {
		return wrapBroker("discovery_start_failed", err, "start discovery provider")
	}
	nodes, err := discovery.WaitForAlive(ctx, b.discovery, b.discoveryBootstrapExpect(), discoveryWaitInterval)
	if err != nil {
		return wrapBroker("discovery_bootstrap_failed", err, "wait for discovery bootstrap")
	}
	peers, err := raftPeersFromDiscovery(b.cfg, nodes)
	if err != nil {
		return err
	}
	b.cfg.Raft.Cluster = peers
	b.configureCommandRuntime()
	if b.logger != nil {
		b.logger.Info("discovery resolved raft peers", "provider", b.cfg.Discovery.Provider, "peers", len(peers))
	}
	return nil
}

func (b *Broker) stopDiscovery(ctx context.Context) error {
	if b == nil || b.discovery == nil {
		return nil
	}
	return wrapBroker("discovery_stop_failed", b.discovery.Stop(ctx), "stop discovery provider")
}

func (b *Broker) discoveryBootstrapExpect() int {
	if b == nil {
		return 1
	}
	if b.cfg.Discovery.BootstrapExpect > 0 {
		return b.cfg.Discovery.BootstrapExpect
	}
	if len(b.cfg.Discovery.Seeds) > 0 {
		return len(b.cfg.Discovery.Seeds) + 1
	}
	return max(1, len(b.cfg.Raft.Cluster))
}

func raftPeersFromDiscovery(cfg Config, nodes []discovery.Node) ([]RaftPeerConfig, error) {
	seen := collectionmapping.NewMap[uint64, RaftPeerConfig]()
	for _, node := range discovery.AliveNodes(nodes) {
		if node.ClusterName != "" && node.ClusterName != cfg.Broker.ClusterName {
			continue
		}
		if !node.ValidRaftPeer() {
			continue
		}
		if existing, ok := seen.Get(node.ID); ok && existing.Addr != node.RaftAddr {
			return nil, brokerStoreError(store.CodeInvalidArgument, "discovery returned duplicate node id %d with raft addresses %q and %q", node.ID, existing.Addr, node.RaftAddr)
		}
		seen.Set(node.ID, RaftPeerConfig{NodeID: node.ID, Addr: node.RaftAddr})
	}
	if _, ok := seen.Get(cfg.Broker.NodeID); !ok {
		return nil, brokerStoreError(store.CodeInvalidArgument, "discovery did not return local node %d as a valid raft peer", cfg.Broker.NodeID)
	}
	out := collectionlist.NewListWithCapacity[RaftPeerConfig](seen.Len())
	seen.Range(func(_ uint64, peer RaftPeerConfig) bool {
		out.Add(peer)
		return true
	})
	return out.Sort(func(left, right RaftPeerConfig) int {
		return cmp.Compare(left.NodeID, right.NodeID)
	}).Values(), nil
}
