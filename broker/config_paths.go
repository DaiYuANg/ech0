package broker

import (
	"path/filepath"
	"strconv"
	"strings"
)

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
