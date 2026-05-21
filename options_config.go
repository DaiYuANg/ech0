package ech0

import (
	"time"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
)

func applyRaftOptions(cfg *internalbroker.Config, opts Options) {
	if opts.Raft.BindAddr != "" {
		cfg.Raft.BindAddr = opts.Raft.BindAddr
	}
	if opts.Raft.ApplyTimeout > 0 {
		cfg.Raft.ApplyTimeoutMS = durationMillis(opts.Raft.ApplyTimeout)
	}
	if opts.Raft.HeartbeatTimeout > 0 {
		cfg.Raft.HeartbeatIntervalMS = durationMillis(opts.Raft.HeartbeatTimeout)
	}
	cfg.Raft.Cluster = make([]internalbroker.RaftPeerConfig, 0, len(opts.Raft.Peers))
	for _, peer := range opts.Raft.Peers {
		cfg.Raft.Cluster = append(cfg.Raft.Cluster, internalbroker.RaftPeerConfig{NodeID: peer.NodeID, Addr: peer.Addr})
	}
	if len(cfg.Raft.Cluster) == 0 {
		cfg.Raft.Cluster = []internalbroker.RaftPeerConfig{{NodeID: opts.NodeID, Addr: cfg.Raft.BindAddr}}
	}
}

func durationMillis(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration / time.Millisecond)
}
