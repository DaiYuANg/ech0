//nolint:revive // Config schema, defaults, and path helpers are kept together for operators.
package broker

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"

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
	NodeID                       uint64 `json:"node_id"                         koanf:"node_id"                         mapstructure:"node_id"                         toml:"node_id"`
	ClusterName                  string `json:"cluster_name"                    koanf:"cluster_name"                    mapstructure:"cluster_name"                    toml:"cluster_name"`
	DataDir                      string `json:"data_dir"                        koanf:"data_dir"                        mapstructure:"data_dir"                        toml:"data_dir"`
	BindAddr                     string `json:"bind_addr"                       koanf:"bind_addr"                       mapstructure:"bind_addr"                       toml:"bind_addr"`
	MaxFrameBodyBytes            uint32 `json:"max_frame_body_bytes"            koanf:"max_frame_body_bytes"            mapstructure:"max_frame_body_bytes"            toml:"max_frame_body_bytes"`
	MaxPayloadBytes              int    `json:"max_payload_bytes"               koanf:"max_payload_bytes"               mapstructure:"max_payload_bytes"               toml:"max_payload_bytes"`
	MaxBatchPayloadBytes         int    `json:"max_batch_payload_bytes"         koanf:"max_batch_payload_bytes"         mapstructure:"max_batch_payload_bytes"         toml:"max_batch_payload_bytes"`
	MaxFetchRecords              int    `json:"max_fetch_records"               koanf:"max_fetch_records"               mapstructure:"max_fetch_records"               toml:"max_fetch_records"`
	MaxFetchWaitMS               uint64 `json:"max_fetch_wait_ms"               koanf:"max_fetch_wait_ms"               mapstructure:"max_fetch_wait_ms"               toml:"max_fetch_wait_ms"`
	GroupAssignmentStrategy      string `json:"group_assignment_strategy"       koanf:"group_assignment_strategy"       mapstructure:"group_assignment_strategy"       toml:"group_assignment_strategy"`
	GroupStickyAssignments       bool   `json:"group_sticky_assignments"        koanf:"group_sticky_assignments"        mapstructure:"group_sticky_assignments"        toml:"group_sticky_assignments"`
	RetryWorkerEnabled           bool   `json:"retry_worker_enabled"            koanf:"retry_worker_enabled"            mapstructure:"retry_worker_enabled"            toml:"retry_worker_enabled"`
	RetryWorkerIntervalSecs      uint64 `json:"retry_worker_interval_secs"      koanf:"retry_worker_interval_secs"      mapstructure:"retry_worker_interval_secs"      toml:"retry_worker_interval_secs"`
	RetryWorkerMaxRecords        int    `json:"retry_worker_max_records"        koanf:"retry_worker_max_records"        mapstructure:"retry_worker_max_records"        toml:"retry_worker_max_records"`
	RetryWorkerConsumerPrefix    string `json:"retry_worker_consumer_prefix"    koanf:"retry_worker_consumer_prefix"    mapstructure:"retry_worker_consumer_prefix"    toml:"retry_worker_consumer_prefix"`
	DelaySchedulerEnabled        bool   `json:"delay_scheduler_enabled"         koanf:"delay_scheduler_enabled"         mapstructure:"delay_scheduler_enabled"         toml:"delay_scheduler_enabled"`
	DelaySchedulerIntervalSecs   uint64 `json:"delay_scheduler_interval_secs"   koanf:"delay_scheduler_interval_secs"   mapstructure:"delay_scheduler_interval_secs"   toml:"delay_scheduler_interval_secs"`
	DelaySchedulerMaxRecords     int    `json:"delay_scheduler_max_records"     koanf:"delay_scheduler_max_records"     mapstructure:"delay_scheduler_max_records"     toml:"delay_scheduler_max_records"`
	DelaySchedulerConsumerPrefix string `json:"delay_scheduler_consumer_prefix" koanf:"delay_scheduler_consumer_prefix" mapstructure:"delay_scheduler_consumer_prefix" toml:"delay_scheduler_consumer_prefix"`
}

type AdminConfig struct {
	Enabled  bool   `json:"enabled"   koanf:"enabled"   mapstructure:"enabled"   toml:"enabled"`
	BindAddr string `json:"bind_addr" koanf:"bind_addr" mapstructure:"bind_addr" toml:"bind_addr"`
}

type StorageConfig struct {
	SegmentsDir  string `json:"segments_dir"  koanf:"segments_dir"  mapstructure:"segments_dir"  toml:"segments_dir"`
	MetadataPath string `json:"metadata_path" koanf:"metadata_path" mapstructure:"metadata_path" toml:"metadata_path"`
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
			GroupAssignmentStrategy:      "round_robin",
			GroupStickyAssignments:       true,
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
			SegmentsDir:  "segments",
			MetadataPath: "meta/metadata.bbolt",
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
			Enabled:              false,
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
	files := make([]string, 0, len(paths))
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
		files = append(files, path)
	}
	cfg, err := configx.LoadTErr[Config](
		configx.WithTypedDefaults(DefaultConfig()),
		configx.WithFiles(files...),
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
	if cfg.MetadataPath == "" {
		cfg.MetadataPath = "meta/metadata.bbolt"
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

func (c Config) DataDir() string {
	if c.Broker.DataDir == "" {
		return "./data"
	}
	return c.Broker.DataDir
}

func (c Config) RaftDir() string {
	return filepath.Join(c.DataDir(), "raft", strconv.FormatUint(c.Broker.NodeID, 10))
}

func (c Config) SegmentLogPath() string {
	dir := c.Storage.SegmentsDir
	if dir == "" {
		dir = "segments"
	}
	if filepath.IsAbs(dir) {
		return filepath.Join(dir, "log.bbolt")
	}
	return filepath.Join(c.DataDir(), dir, "log.bbolt")
}

func (c Config) MetadataPath() string {
	path := c.Storage.MetadataPath
	if path == "" {
		path = "meta/metadata.bbolt"
	}
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(c.DataDir(), path)
}

func (c Config) LogFilePath() string {
	return filepath.Join(c.DataDir(), c.Logging.Directory, c.Logging.FilePrefix+".log")
}

func configFlagName(name string) string {
	switch name {
	case "broker-addr":
		return "broker.bind_addr"
	case "admin-addr":
		return "admin.bind_addr"
	case "data-dir":
		return "broker.data_dir"
	case "raft":
		return "raft.enabled"
	default:
		return strings.ReplaceAll(strings.ToLower(name), "-", "_")
	}
}
