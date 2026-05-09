package broker

import (
	"path/filepath"
	"strconv"
	"strings"

	"github.com/DaiYuANg/ech0/store"
)

func (c Config) DataDir() string {
	if c.Broker.DataDir == "" {
		return "./data"
	}
	return c.Broker.DataDir
}

func (c Config) DragonboatDir() string {
	return filepath.Join(c.DataDir(), "dragonboat", strconv.FormatUint(c.Broker.NodeID, 10))
}

func (c Config) MetadataDir() string {
	return filepath.Join(c.DataDir(), "metadata")
}

func (c Config) ShardsDir() string {
	return filepath.Join(c.DataDir(), "shards")
}

func (c Config) ShardDir(shardID store.ShardID) string {
	return filepath.Join(c.ShardsDir(), shardDirectoryName(shardID))
}

func (c Config) ShardSegmentLogPath(shardID store.ShardID) string {
	return filepath.Join(c.ShardDir(shardID), "segments")
}

func (c Config) ShardBadgerPath(shardID store.ShardID) string {
	return filepath.Join(c.ShardDir(shardID), "badger")
}

func (c Config) SegmentLogPath() string {
	dir := c.Storage.SegmentsDir
	if dir == "" {
		dir = "segments"
	}
	if filepath.IsAbs(dir) {
		return dir
	}
	return filepath.Join(c.DataDir(), dir)
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

func shardDirectoryName(shardID store.ShardID) string {
	value := strconv.FormatUint(uint64(shardID), 10)
	if len(value) < 4 {
		value = strings.Repeat("0", 4-len(value)) + value
	}
	return "shard-" + value
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
