package broker

func DefaultConfig() Config {
	return Config{
		Broker: BrokerConfig{
			NodeID:                       1,
			ClusterName:                  "ech0-dev",
			DataDir:                      "./data",
			BindAddr:                     "127.0.0.1:9090",
			MaxFrameBodyBytes:            4 * 1024 * 1024,
			MaxPayloadBytes:              1024 * 1024,
			MaxBatchPayloadBytes:         8 * 1024 * 1024,
			MaxFetchRecords:              1000,
			MaxFetchWaitMS:               5000,
			MaxConcurrentConnections:     4096,
			CommandRateLimitPerSecond:    0,
			CommandRateLimitBurst:        0,
			TopicCacheMaxEntries:         4096,
			MaintenanceConcurrency:       4,
			GroupAssignmentStrategy:      "round_robin",
			GroupStickyAssignments:       true,
			DataShardCount:               1,
			RetryWorkerEnabled:           true,
			RetryWorkerIntervalSecs:      5,
			RetryWorkerMaxRecords:        256,
			RetryWorkerConsumerPrefix:    "__retry_worker",
			DelaySchedulerEnabled:        true,
			DelaySchedulerIntervalSecs:   1,
			DelaySchedulerMaxRecords:     256,
			DelaySchedulerConsumerPrefix: "__delay_scheduler",
		},
		Admin: AdminConfig{
			Enabled:  true,
			BindAddr: "127.0.0.1:9091",
		},
		Storage: StorageConfig{
			SegmentsDir:                   "segments",
			SegmentReadMode:               "pread",
			RetentionCleanupEnabled:       true,
			RetentionCleanupIntervalSecs:  30,
			CompactionCleanupEnabled:      true,
			CompactionCleanupIntervalSecs: 60,
			CompactionSealedSegmentBatch:  2,
		},
		Logging: LoggingConfig{
			Level:        "info",
			Format:       "text",
			EnableStdout: true,
			EnableFile:   false,
			Directory:    "logs",
			FilePrefix:   "ech0",
			ANSI:         true,
		},
		Raft: RaftConfig{
			BindAddr:             "127.0.0.1:3210",
			ReadPolicy:           RaftReadLocal,
			HeartbeatIntervalMS:  150,
			ElectionTimeoutMinMS: 300,
			ElectionTimeoutMaxMS: 600,
			ApplyTimeoutMS:       5000,
			Cluster: []RaftPeerConfig{
				{NodeID: 1, Addr: "127.0.0.1:3210"},
			},
		},
	}
}

func normalizeConfig(cfg *Config) {
	normalizeBrokerIdentity(&cfg.Broker)
	normalizeBrokerLimits(&cfg.Broker)
	normalizeBrokerWorkers(&cfg.Broker)
	normalizeAdminConfig(&cfg.Admin)
	normalizeStorageConfig(&cfg.Storage)
	normalizeLoggingConfig(&cfg.Logging)
	normalizeRaftConfig(&cfg.Raft, cfg.Broker)
}

func normalizeBrokerIdentity(cfg *BrokerConfig) {
	if cfg.NodeID == 0 {
		cfg.NodeID = 1
	}
	if cfg.ClusterName == "" {
		cfg.ClusterName = "ech0-dev"
	}
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.BindAddr == "" {
		cfg.BindAddr = "127.0.0.1:9090"
	}
}

func normalizeBrokerLimits(cfg *BrokerConfig) {
	normalizeBrokerPayloadLimits(cfg)
	normalizeBrokerRuntimeLimits(cfg)
}

func normalizeBrokerPayloadLimits(cfg *BrokerConfig) {
	if cfg.MaxFrameBodyBytes == 0 {
		cfg.MaxFrameBodyBytes = 4 * 1024 * 1024
	}
	if cfg.MaxPayloadBytes == 0 {
		cfg.MaxPayloadBytes = 1024 * 1024
	}
	if cfg.MaxBatchPayloadBytes == 0 {
		cfg.MaxBatchPayloadBytes = 8 * 1024 * 1024
	}
	if cfg.MaxFetchRecords == 0 {
		cfg.MaxFetchRecords = 1000
	}
	if cfg.MaxFetchWaitMS == 0 {
		cfg.MaxFetchWaitMS = 5000
	}
}

func normalizeBrokerRuntimeLimits(cfg *BrokerConfig) {
	if cfg.MaxConcurrentConnections == 0 {
		cfg.MaxConcurrentConnections = 4096
	}
	if cfg.CommandRateLimitPerSecond < 0 {
		cfg.CommandRateLimitPerSecond = 0
	}
	if cfg.CommandRateLimitBurst < 0 {
		cfg.CommandRateLimitBurst = 0
	}
	if cfg.CommandRateLimitPerSecond > 0 && cfg.CommandRateLimitBurst == 0 {
		cfg.CommandRateLimitBurst = int(cfg.CommandRateLimitPerSecond)
	}
	if cfg.TopicCacheMaxEntries == 0 {
		cfg.TopicCacheMaxEntries = 4096
	}
	if cfg.MaintenanceConcurrency == 0 {
		cfg.MaintenanceConcurrency = 4
	}
	if cfg.DataShardCount == 0 {
		cfg.DataShardCount = 1
	}
}

func normalizeBrokerWorkers(cfg *BrokerConfig) {
	if cfg.RetryWorkerIntervalSecs == 0 {
		cfg.RetryWorkerIntervalSecs = 5
	}
	if cfg.RetryWorkerMaxRecords == 0 {
		cfg.RetryWorkerMaxRecords = 256
	}
	if cfg.RetryWorkerConsumerPrefix == "" {
		cfg.RetryWorkerConsumerPrefix = "__retry_worker"
	}
	if cfg.DelaySchedulerIntervalSecs == 0 {
		cfg.DelaySchedulerIntervalSecs = 1
	}
	if cfg.DelaySchedulerMaxRecords == 0 {
		cfg.DelaySchedulerMaxRecords = 256
	}
	if cfg.DelaySchedulerConsumerPrefix == "" {
		cfg.DelaySchedulerConsumerPrefix = "__delay_scheduler"
	}
}

func normalizeAdminConfig(cfg *AdminConfig) {
	if cfg.BindAddr == "" {
		cfg.BindAddr = "127.0.0.1:9091"
	}
}

func normalizeStorageConfig(cfg *StorageConfig) {
	if cfg.SegmentsDir == "" {
		cfg.SegmentsDir = "segments"
	}
	if cfg.SegmentReadMode == "" {
		cfg.SegmentReadMode = "pread"
	}
	if cfg.RetentionCleanupIntervalSecs == 0 {
		cfg.RetentionCleanupIntervalSecs = 30
	}
	if cfg.CompactionCleanupIntervalSecs == 0 {
		cfg.CompactionCleanupIntervalSecs = 60
	}
	if cfg.CompactionSealedSegmentBatch <= 0 {
		cfg.CompactionSealedSegmentBatch = 2
	}
}

func normalizeLoggingConfig(cfg *LoggingConfig) {
	if cfg.Level == "" {
		cfg.Level = "info"
	}
	if cfg.Directory == "" {
		cfg.Directory = "logs"
	}
	if cfg.FilePrefix == "" {
		cfg.FilePrefix = "ech0"
	}
}

func normalizeRaftConfig(cfg *RaftConfig, broker BrokerConfig) {
	if cfg.BindAddr == "" {
		cfg.BindAddr = "127.0.0.1:3210"
	}
	if cfg.ReadPolicy == "" {
		cfg.ReadPolicy = RaftReadLocal
	}
	if cfg.HeartbeatIntervalMS == 0 {
		cfg.HeartbeatIntervalMS = 150
	}
	if cfg.ElectionTimeoutMinMS == 0 {
		cfg.ElectionTimeoutMinMS = 300
	}
	if cfg.ElectionTimeoutMaxMS == 0 {
		cfg.ElectionTimeoutMaxMS = 600
	}
	if cfg.ApplyTimeoutMS == 0 {
		cfg.ApplyTimeoutMS = 5000
	}
	if len(cfg.Cluster) == 0 {
		cfg.Cluster = []RaftPeerConfig{{NodeID: broker.NodeID, Addr: cfg.BindAddr}}
	}
}
