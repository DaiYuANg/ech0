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
	Broker  BrokerConfig  `toml:"broker" json:"broker" mapstructure:"broker" koanf:"broker"`
	Admin   AdminConfig   `toml:"admin" json:"admin" mapstructure:"admin" koanf:"admin"`
	Storage StorageConfig `toml:"storage" json:"storage" mapstructure:"storage" koanf:"storage"`
	Logging LoggingConfig `toml:"logging" json:"logging" mapstructure:"logging" koanf:"logging"`
	Raft    RaftConfig    `toml:"raft" json:"raft" mapstructure:"raft" koanf:"raft"`
}

type BrokerConfig struct {
	NodeID                       uint64 `toml:"node_id" json:"node_id" mapstructure:"node_id" koanf:"node_id"`
	ClusterName                  string `toml:"cluster_name" json:"cluster_name" mapstructure:"cluster_name" koanf:"cluster_name"`
	DataDir                      string `toml:"data_dir" json:"data_dir" mapstructure:"data_dir" koanf:"data_dir"`
	BindAddr                     string `toml:"bind_addr" json:"bind_addr" mapstructure:"bind_addr" koanf:"bind_addr"`
	MaxFrameBodyBytes            uint32 `toml:"max_frame_body_bytes" json:"max_frame_body_bytes" mapstructure:"max_frame_body_bytes" koanf:"max_frame_body_bytes"`
	MaxPayloadBytes              int    `toml:"max_payload_bytes" json:"max_payload_bytes" mapstructure:"max_payload_bytes" koanf:"max_payload_bytes"`
	MaxBatchPayloadBytes         int    `toml:"max_batch_payload_bytes" json:"max_batch_payload_bytes" mapstructure:"max_batch_payload_bytes" koanf:"max_batch_payload_bytes"`
	MaxFetchRecords              int    `toml:"max_fetch_records" json:"max_fetch_records" mapstructure:"max_fetch_records" koanf:"max_fetch_records"`
	MaxFetchWaitMS               uint64 `toml:"max_fetch_wait_ms" json:"max_fetch_wait_ms" mapstructure:"max_fetch_wait_ms" koanf:"max_fetch_wait_ms"`
	GroupAssignmentStrategy      string `toml:"group_assignment_strategy" json:"group_assignment_strategy" mapstructure:"group_assignment_strategy" koanf:"group_assignment_strategy"`
	GroupStickyAssignments       bool   `toml:"group_sticky_assignments" json:"group_sticky_assignments" mapstructure:"group_sticky_assignments" koanf:"group_sticky_assignments"`
	RetryWorkerEnabled           bool   `toml:"retry_worker_enabled" json:"retry_worker_enabled" mapstructure:"retry_worker_enabled" koanf:"retry_worker_enabled"`
	RetryWorkerIntervalSecs      uint64 `toml:"retry_worker_interval_secs" json:"retry_worker_interval_secs" mapstructure:"retry_worker_interval_secs" koanf:"retry_worker_interval_secs"`
	RetryWorkerMaxRecords        int    `toml:"retry_worker_max_records" json:"retry_worker_max_records" mapstructure:"retry_worker_max_records" koanf:"retry_worker_max_records"`
	RetryWorkerConsumerPrefix    string `toml:"retry_worker_consumer_prefix" json:"retry_worker_consumer_prefix" mapstructure:"retry_worker_consumer_prefix" koanf:"retry_worker_consumer_prefix"`
	DelaySchedulerEnabled        bool   `toml:"delay_scheduler_enabled" json:"delay_scheduler_enabled" mapstructure:"delay_scheduler_enabled" koanf:"delay_scheduler_enabled"`
	DelaySchedulerIntervalSecs   uint64 `toml:"delay_scheduler_interval_secs" json:"delay_scheduler_interval_secs" mapstructure:"delay_scheduler_interval_secs" koanf:"delay_scheduler_interval_secs"`
	DelaySchedulerMaxRecords     int    `toml:"delay_scheduler_max_records" json:"delay_scheduler_max_records" mapstructure:"delay_scheduler_max_records" koanf:"delay_scheduler_max_records"`
	DelaySchedulerConsumerPrefix string `toml:"delay_scheduler_consumer_prefix" json:"delay_scheduler_consumer_prefix" mapstructure:"delay_scheduler_consumer_prefix" koanf:"delay_scheduler_consumer_prefix"`
}

type AdminConfig struct {
	Enabled  bool   `toml:"enabled" json:"enabled" mapstructure:"enabled" koanf:"enabled"`
	BindAddr string `toml:"bind_addr" json:"bind_addr" mapstructure:"bind_addr" koanf:"bind_addr"`
}

type StorageConfig struct {
	SegmentsDir  string `toml:"segments_dir" json:"segments_dir" mapstructure:"segments_dir" koanf:"segments_dir"`
	MetadataPath string `toml:"metadata_path" json:"metadata_path" mapstructure:"metadata_path" koanf:"metadata_path"`
}

type LoggingConfig struct {
	Level        string `toml:"level" json:"level" mapstructure:"level" koanf:"level"`
	Format       string `toml:"format" json:"format" mapstructure:"format" koanf:"format"`
	EnableStdout bool   `toml:"enable_stdout" json:"enable_stdout" mapstructure:"enable_stdout" koanf:"enable_stdout"`
	EnableFile   bool   `toml:"enable_file" json:"enable_file" mapstructure:"enable_file" koanf:"enable_file"`
	Directory    string `toml:"directory" json:"directory" mapstructure:"directory" koanf:"directory"`
	FilePrefix   string `toml:"file_prefix" json:"file_prefix" mapstructure:"file_prefix" koanf:"file_prefix"`
	ANSI         bool   `toml:"ansi" json:"ansi" mapstructure:"ansi" koanf:"ansi"`
}

type RaftReadPolicy string

const (
	RaftReadLocal        RaftReadPolicy = "local"
	RaftReadLeader       RaftReadPolicy = "leader"
	RaftReadLinearizable RaftReadPolicy = "linearizable"
)

type RaftConfig struct {
	Enabled              bool             `toml:"enabled" json:"enabled" mapstructure:"enabled" koanf:"enabled"`
	BindAddr             string           `toml:"bind_addr" json:"bind_addr" mapstructure:"bind_addr" koanf:"bind_addr"`
	ReadPolicy           RaftReadPolicy   `toml:"read_policy" json:"read_policy" mapstructure:"read_policy" koanf:"read_policy"`
	HeartbeatIntervalMS  uint64           `toml:"heartbeat_interval_ms" json:"heartbeat_interval_ms" mapstructure:"heartbeat_interval_ms" koanf:"heartbeat_interval_ms"`
	ElectionTimeoutMinMS uint64           `toml:"election_timeout_min_ms" json:"election_timeout_min_ms" mapstructure:"election_timeout_min_ms" koanf:"election_timeout_min_ms"`
	ElectionTimeoutMaxMS uint64           `toml:"election_timeout_max_ms" json:"election_timeout_max_ms" mapstructure:"election_timeout_max_ms" koanf:"election_timeout_max_ms"`
	ApplyTimeoutMS       uint64           `toml:"apply_timeout_ms" json:"apply_timeout_ms" mapstructure:"apply_timeout_ms" koanf:"apply_timeout_ms"`
	Cluster              []RaftPeerConfig `toml:"cluster" json:"cluster" mapstructure:"cluster" koanf:"cluster"`
}

type RaftPeerConfig struct {
	NodeID uint64 `toml:"node_id" json:"node_id" mapstructure:"node_id" koanf:"node_id"`
	Addr   string `toml:"addr" json:"addr" mapstructure:"addr" koanf:"addr"`
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
			return Config{}, err
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
	if cfg.Broker.NodeID == 0 {
		cfg.Broker.NodeID = 1
	}
	if cfg.Broker.ClusterName == "" {
		cfg.Broker.ClusterName = "ech0-dev"
	}
	if cfg.Broker.DataDir == "" {
		cfg.Broker.DataDir = "./data"
	}
	if cfg.Broker.BindAddr == "" {
		cfg.Broker.BindAddr = "127.0.0.1:9090"
	}
	if cfg.Broker.MaxFrameBodyBytes == 0 {
		cfg.Broker.MaxFrameBodyBytes = 4 * 1024 * 1024
	}
	if cfg.Broker.MaxPayloadBytes == 0 {
		cfg.Broker.MaxPayloadBytes = 1024 * 1024
	}
	if cfg.Broker.MaxBatchPayloadBytes == 0 {
		cfg.Broker.MaxBatchPayloadBytes = 8 * 1024 * 1024
	}
	if cfg.Broker.MaxFetchRecords == 0 {
		cfg.Broker.MaxFetchRecords = 1000
	}
	if cfg.Broker.MaxFetchWaitMS == 0 {
		cfg.Broker.MaxFetchWaitMS = 5000
	}
	if cfg.Broker.RetryWorkerIntervalSecs == 0 {
		cfg.Broker.RetryWorkerIntervalSecs = 5
	}
	if cfg.Broker.RetryWorkerMaxRecords == 0 {
		cfg.Broker.RetryWorkerMaxRecords = 256
	}
	if cfg.Broker.RetryWorkerConsumerPrefix == "" {
		cfg.Broker.RetryWorkerConsumerPrefix = "__retry_worker"
	}
	if cfg.Broker.DelaySchedulerIntervalSecs == 0 {
		cfg.Broker.DelaySchedulerIntervalSecs = 1
	}
	if cfg.Broker.DelaySchedulerMaxRecords == 0 {
		cfg.Broker.DelaySchedulerMaxRecords = 256
	}
	if cfg.Broker.DelaySchedulerConsumerPrefix == "" {
		cfg.Broker.DelaySchedulerConsumerPrefix = "__delay_scheduler"
	}
	if cfg.Admin.BindAddr == "" {
		cfg.Admin.BindAddr = "127.0.0.1:9091"
	}
	if cfg.Storage.SegmentsDir == "" {
		cfg.Storage.SegmentsDir = "segments"
	}
	if cfg.Storage.MetadataPath == "" {
		cfg.Storage.MetadataPath = "meta/metadata.bbolt"
	}
	if cfg.Logging.Level == "" {
		cfg.Logging.Level = "info"
	}
	if cfg.Logging.Directory == "" {
		cfg.Logging.Directory = "logs"
	}
	if cfg.Logging.FilePrefix == "" {
		cfg.Logging.FilePrefix = "ech0"
	}
	if cfg.Raft.BindAddr == "" {
		cfg.Raft.BindAddr = "127.0.0.1:3210"
	}
	if cfg.Raft.ReadPolicy == "" {
		cfg.Raft.ReadPolicy = RaftReadLocal
	}
	if cfg.Raft.HeartbeatIntervalMS == 0 {
		cfg.Raft.HeartbeatIntervalMS = 150
	}
	if cfg.Raft.ElectionTimeoutMinMS == 0 {
		cfg.Raft.ElectionTimeoutMinMS = 300
	}
	if cfg.Raft.ElectionTimeoutMaxMS == 0 {
		cfg.Raft.ElectionTimeoutMaxMS = 600
	}
	if cfg.Raft.ApplyTimeoutMS == 0 {
		cfg.Raft.ApplyTimeoutMS = 5000
	}
	if len(cfg.Raft.Cluster) == 0 {
		cfg.Raft.Cluster = []RaftPeerConfig{{NodeID: cfg.Broker.NodeID, Addr: cfg.Raft.BindAddr}}
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
