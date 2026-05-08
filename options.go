package ech0

import (
	"time"

	internalbroker "github.com/DaiYuANg/ech0/broker"
)

type Options struct {
	DataDir        string
	NodeID         uint64
	Raft           *RaftOptions
	DisableRetry   bool
	DisableDelay   bool
	MaxFetch       int
	MaxPayloadSize int
}

type RaftOptions struct {
	BindAddr         string
	CommitTimeout    time.Duration
	ApplyTimeout     time.Duration
	HeartbeatTimeout time.Duration
	Peers            []RaftPeer
}

type RaftPeer struct {
	NodeID uint64
	Addr   string
}

type TopicOption func(*topicOptions)

type topicOptions struct {
	partitions      uint32
	delayEnabled    bool
	deadLetterTopic string
	retryPolicy     *RetryPolicy
}

type RetryPolicy struct {
	MaxAttempts    uint32
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

type PublishOption func(*publishOptions)

type publishOptions struct {
	key       []byte
	partition *uint32
	tombstone bool
}

type FetchOption func(*fetchOptions)

type fetchOptions struct {
	partition  uint32
	offset     *uint64
	maxRecords int
}

func DefaultOptions() Options {
	return Options{
		DataDir:        "./data",
		NodeID:         1,
		MaxFetch:       1000,
		MaxPayloadSize: 1024 * 1024,
	}
}

func Partitions(n uint32) TopicOption {
	return func(opts *topicOptions) {
		if n > 0 {
			opts.partitions = n
		}
	}
}

func EnableDelay() TopicOption {
	return func(opts *topicOptions) {
		opts.delayEnabled = true
	}
}

func DeadLetterTopic(topic string) TopicOption {
	return func(opts *topicOptions) {
		opts.deadLetterTopic = topic
	}
}

func Retry(policy RetryPolicy) TopicOption {
	return func(opts *topicOptions) {
		opts.retryPolicy = &policy
	}
}

func Key(key []byte) PublishOption {
	return func(opts *publishOptions) {
		opts.key = append([]byte(nil), key...)
	}
}

func Partition(partition uint32) PublishOption {
	return func(opts *publishOptions) {
		opts.partition = &partition
	}
}

func Tombstone() PublishOption {
	return func(opts *publishOptions) {
		opts.tombstone = true
	}
}

func FetchPartition(partition uint32) FetchOption {
	return func(opts *fetchOptions) {
		opts.partition = partition
	}
}

func FetchOffset(offset uint64) FetchOption {
	return func(opts *fetchOptions) {
		opts.offset = &offset
	}
}

func FetchLimit(maxRecords int) FetchOption {
	return func(opts *fetchOptions) {
		if maxRecords > 0 {
			opts.maxRecords = maxRecords
		}
	}
}

func normalizeOptions(opts Options) Options {
	defaults := DefaultOptions()
	if opts.DataDir == "" {
		opts.DataDir = defaults.DataDir
	}
	if opts.NodeID == 0 {
		opts.NodeID = defaults.NodeID
	}
	if opts.MaxFetch == 0 {
		opts.MaxFetch = defaults.MaxFetch
	}
	if opts.MaxPayloadSize == 0 {
		opts.MaxPayloadSize = defaults.MaxPayloadSize
	}
	return opts
}

func configFromOptions(opts Options) internalbroker.Config {
	cfg := internalbroker.DefaultConfig()
	cfg.Broker.NodeID = opts.NodeID
	cfg.Broker.DataDir = opts.DataDir
	cfg.Broker.MaxFetchRecords = opts.MaxFetch
	cfg.Broker.MaxPayloadBytes = opts.MaxPayloadSize
	cfg.Admin.Enabled = false
	cfg.Broker.RetryWorkerEnabled = !opts.DisableRetry
	cfg.Broker.DelaySchedulerEnabled = !opts.DisableDelay
	cfg.Raft.Enabled = opts.Raft != nil
	if opts.Raft != nil {
		applyRaftOptions(&cfg, opts)
	}
	return cfg
}

func applyRaftOptions(cfg *internalbroker.Config, opts Options) {
	if opts.Raft.BindAddr != "" {
		cfg.Raft.BindAddr = opts.Raft.BindAddr
	}
	if opts.Raft.CommitTimeout > 0 {
		cfg.Raft.CommitTimeoutMS = durationMillis(opts.Raft.CommitTimeout)
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
