package broker

import (
	"errors"
	"os"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/configx"
	"github.com/spf13/pflag"
)

type Config struct {
	Broker  BrokerConfig  `json:"broker"  koanf:"broker"  mapstructure:"broker"  toml:"broker"`
	Admin   AdminConfig   `json:"admin"   koanf:"admin"   mapstructure:"admin"   toml:"admin"`
	Storage StorageConfig `json:"storage" koanf:"storage" mapstructure:"storage" toml:"storage"`
	Logging LoggingConfig `json:"logging" koanf:"logging" mapstructure:"logging" toml:"logging"`
	Raft    RaftConfig    `json:"raft"    koanf:"raft"    mapstructure:"raft"    toml:"raft"`
}

type BrokerConfig struct {
	NodeID                       uint64  `json:"node_id"                         koanf:"node_id"                         mapstructure:"node_id"                         toml:"node_id"`
	ClusterName                  string  `json:"cluster_name"                    koanf:"cluster_name"                    mapstructure:"cluster_name"                    toml:"cluster_name"`
	DataDir                      string  `json:"data_dir"                        koanf:"data_dir"                        mapstructure:"data_dir"                        toml:"data_dir"`
	BindAddr                     string  `json:"bind_addr"                       koanf:"bind_addr"                       mapstructure:"bind_addr"                       toml:"bind_addr"`
	MaxFrameBodyBytes            uint32  `json:"max_frame_body_bytes"            koanf:"max_frame_body_bytes"            mapstructure:"max_frame_body_bytes"            toml:"max_frame_body_bytes"`
	MaxPayloadBytes              int     `json:"max_payload_bytes"               koanf:"max_payload_bytes"               mapstructure:"max_payload_bytes"               toml:"max_payload_bytes"`
	MaxBatchPayloadBytes         int     `json:"max_batch_payload_bytes"         koanf:"max_batch_payload_bytes"         mapstructure:"max_batch_payload_bytes"         toml:"max_batch_payload_bytes"`
	MaxFetchRecords              int     `json:"max_fetch_records"               koanf:"max_fetch_records"               mapstructure:"max_fetch_records"               toml:"max_fetch_records"`
	MaxFetchWaitMS               uint64  `json:"max_fetch_wait_ms"               koanf:"max_fetch_wait_ms"               mapstructure:"max_fetch_wait_ms"               toml:"max_fetch_wait_ms"`
	MaxConcurrentConnections     int64   `json:"max_concurrent_connections"      koanf:"max_concurrent_connections"      mapstructure:"max_concurrent_connections"      toml:"max_concurrent_connections"`
	CommandRateLimitPerSecond    float64 `json:"command_rate_limit_per_second"   koanf:"command_rate_limit_per_second"   mapstructure:"command_rate_limit_per_second"   toml:"command_rate_limit_per_second"`
	CommandRateLimitBurst        int     `json:"command_rate_limit_burst"        koanf:"command_rate_limit_burst"        mapstructure:"command_rate_limit_burst"        toml:"command_rate_limit_burst"`
	TopicCacheMaxEntries         int64   `json:"topic_cache_max_entries"         koanf:"topic_cache_max_entries"         mapstructure:"topic_cache_max_entries"         toml:"topic_cache_max_entries"`
	MaintenanceConcurrency       int64   `json:"maintenance_concurrency"         koanf:"maintenance_concurrency"         mapstructure:"maintenance_concurrency"         toml:"maintenance_concurrency"`
	GroupAssignmentStrategy      string  `json:"group_assignment_strategy"       koanf:"group_assignment_strategy"       mapstructure:"group_assignment_strategy"       toml:"group_assignment_strategy"`
	GroupStickyAssignments       bool    `json:"group_sticky_assignments"        koanf:"group_sticky_assignments"        mapstructure:"group_sticky_assignments"        toml:"group_sticky_assignments"`
	DataShardCount               uint32  `json:"data_shard_count"                koanf:"data_shard_count"                mapstructure:"data_shard_count"                toml:"data_shard_count"`
	RetryWorkerEnabled           bool    `json:"retry_worker_enabled"            koanf:"retry_worker_enabled"            mapstructure:"retry_worker_enabled"            toml:"retry_worker_enabled"`
	RetryWorkerIntervalSecs      uint64  `json:"retry_worker_interval_secs"      koanf:"retry_worker_interval_secs"      mapstructure:"retry_worker_interval_secs"      toml:"retry_worker_interval_secs"`
	RetryWorkerMaxRecords        int     `json:"retry_worker_max_records"        koanf:"retry_worker_max_records"        mapstructure:"retry_worker_max_records"        toml:"retry_worker_max_records"`
	RetryWorkerConsumerPrefix    string  `json:"retry_worker_consumer_prefix"    koanf:"retry_worker_consumer_prefix"    mapstructure:"retry_worker_consumer_prefix"    toml:"retry_worker_consumer_prefix"`
	DelaySchedulerEnabled        bool    `json:"delay_scheduler_enabled"         koanf:"delay_scheduler_enabled"         mapstructure:"delay_scheduler_enabled"         toml:"delay_scheduler_enabled"`
	DelaySchedulerIntervalSecs   uint64  `json:"delay_scheduler_interval_secs"   koanf:"delay_scheduler_interval_secs"   mapstructure:"delay_scheduler_interval_secs"   toml:"delay_scheduler_interval_secs"`
	DelaySchedulerMaxRecords     int     `json:"delay_scheduler_max_records"     koanf:"delay_scheduler_max_records"     mapstructure:"delay_scheduler_max_records"     toml:"delay_scheduler_max_records"`
	DelaySchedulerConsumerPrefix string  `json:"delay_scheduler_consumer_prefix" koanf:"delay_scheduler_consumer_prefix" mapstructure:"delay_scheduler_consumer_prefix" toml:"delay_scheduler_consumer_prefix"`
}

type AdminConfig struct {
	Enabled      bool   `json:"enabled"       koanf:"enabled"       mapstructure:"enabled"       toml:"enabled"`
	BindAddr     string `json:"bind_addr"     koanf:"bind_addr"     mapstructure:"bind_addr"     toml:"bind_addr"`
	DebugEnabled bool   `json:"debug_enabled" koanf:"debug_enabled" mapstructure:"debug_enabled" toml:"debug_enabled"`
}

type StorageConfig struct {
	SegmentsDir                   string `json:"segments_dir"                     koanf:"segments_dir"                     mapstructure:"segments_dir"                     toml:"segments_dir"`
	MetadataPath                  string `json:"metadata_path"                    koanf:"metadata_path"                    mapstructure:"metadata_path"                    toml:"metadata_path"`
	RetentionCleanupEnabled       bool   `json:"retention_cleanup_enabled"        koanf:"retention_cleanup_enabled"        mapstructure:"retention_cleanup_enabled"        toml:"retention_cleanup_enabled"`
	RetentionCleanupIntervalSecs  uint64 `json:"retention_cleanup_interval_secs"  koanf:"retention_cleanup_interval_secs"  mapstructure:"retention_cleanup_interval_secs"  toml:"retention_cleanup_interval_secs"`
	CompactionCleanupEnabled      bool   `json:"compaction_cleanup_enabled"       koanf:"compaction_cleanup_enabled"       mapstructure:"compaction_cleanup_enabled"       toml:"compaction_cleanup_enabled"`
	CompactionCleanupIntervalSecs uint64 `json:"compaction_cleanup_interval_secs" koanf:"compaction_cleanup_interval_secs" mapstructure:"compaction_cleanup_interval_secs" toml:"compaction_cleanup_interval_secs"`
	CompactionSealedSegmentBatch  int    `json:"compaction_sealed_segment_batch"  koanf:"compaction_sealed_segment_batch"  mapstructure:"compaction_sealed_segment_batch"  toml:"compaction_sealed_segment_batch"`
}

type LoggingConfig struct {
	Level        string `json:"level"         koanf:"level"         mapstructure:"level"         toml:"level"`
	Format       string `json:"format"        koanf:"format"        mapstructure:"format"        toml:"format"`
	EnableStdout bool   `json:"enable_stdout" koanf:"enable_stdout" mapstructure:"enable_stdout" toml:"enable_stdout"`
	EnableFile   bool   `json:"enable_file"   koanf:"enable_file"   mapstructure:"enable_file"   toml:"enable_file"`
	Directory    string `json:"directory"     koanf:"directory"     mapstructure:"directory"     toml:"directory"`
	FilePrefix   string `json:"file_prefix"   koanf:"file_prefix"   mapstructure:"file_prefix"   toml:"file_prefix"`
	ANSI         bool   `json:"ansi"          koanf:"ansi"          mapstructure:"ansi"          toml:"ansi"`
}

type RaftReadPolicy string

const (
	RaftReadLocal        RaftReadPolicy = "local"
	RaftReadLeader       RaftReadPolicy = "leader"
	RaftReadLinearizable RaftReadPolicy = "linearizable"
)

type RaftConfig struct {
	Enabled              bool             `json:"enabled"                 koanf:"enabled"                 mapstructure:"enabled"                 toml:"enabled"`
	BindAddr             string           `json:"bind_addr"               koanf:"bind_addr"               mapstructure:"bind_addr"               toml:"bind_addr"`
	ReadPolicy           RaftReadPolicy   `json:"read_policy"             koanf:"read_policy"             mapstructure:"read_policy"             toml:"read_policy"`
	HeartbeatIntervalMS  uint64           `json:"heartbeat_interval_ms"   koanf:"heartbeat_interval_ms"   mapstructure:"heartbeat_interval_ms"   toml:"heartbeat_interval_ms"`
	ElectionTimeoutMinMS uint64           `json:"election_timeout_min_ms" koanf:"election_timeout_min_ms" mapstructure:"election_timeout_min_ms" toml:"election_timeout_min_ms"`
	ElectionTimeoutMaxMS uint64           `json:"election_timeout_max_ms" koanf:"election_timeout_max_ms" mapstructure:"election_timeout_max_ms" toml:"election_timeout_max_ms"`
	ApplyTimeoutMS       uint64           `json:"apply_timeout_ms"        koanf:"apply_timeout_ms"        mapstructure:"apply_timeout_ms"        toml:"apply_timeout_ms"`
	Cluster              []RaftPeerConfig `json:"cluster"                 koanf:"cluster"                 mapstructure:"cluster"                 toml:"cluster"`
}

type RaftPeerConfig struct {
	NodeID uint64 `json:"node_id" koanf:"node_id" mapstructure:"node_id" toml:"node_id"`
	Addr   string `json:"addr"    koanf:"addr"    mapstructure:"addr"    toml:"addr"`
}

func LoadConfig(paths ...string) (Config, error) {
	if len(paths) == 0 {
		paths = []string{"config/ech0.toml", "config/ech0.local.toml"}
	}
	return loadConfig(paths, nil)
}

func LoadConfigFromFlagSet(flags *pflag.FlagSet, paths ...string) (Config, error) {
	if len(paths) == 0 {
		paths = []string{"config/ech0.toml", "config/ech0.local.toml"}
	}
	return loadConfig(paths, flags)
}

func loadConfig(paths []string, flags *pflag.FlagSet) (Config, error) {
	files := collectionlist.NewListWithCapacity[string](len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return Config{}, wrapBroker("config_stat_failed", err, "stat config file %s", path)
		}
		files.Add(path)
	}
	cfg, err := configx.LoadTErr[Config](
		configx.WithTypedDefaults(DefaultConfig()),
		configx.WithFiles(files.Values()...),
		configx.WithEnvPrefix("ECH0"),
		configx.WithEnvSeparator("__"),
		configx.WithFlagSet(flags),
		configx.WithArgsNameFunc(configFlagName),
	)
	if err != nil {
		return cfg, err
	}
	normalizeConfig(&cfg)
	return cfg, nil
}
