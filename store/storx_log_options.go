package store

import (
	"strings"
)

type SegmentCompression string
type SegmentReadMode string

const (
	SegmentCompressionNone SegmentCompression = "none"
	SegmentCompressionZstd SegmentCompression = "zstd"
	SegmentReadModePread   SegmentReadMode    = "pread"
	SegmentReadModeMmap    SegmentReadMode    = "mmap"
)

type StorxLogOptions struct {
	Compression SegmentCompression
	ReadMode    SegmentReadMode
	Metrics     StoreMetrics
}

func (o StorxLogOptions) normalize() (StorxLogOptions, error) {
	compression, err := normalizeSegmentCompression(o.Compression)
	if err != nil {
		return StorxLogOptions{}, err
	}
	o.Compression = compression
	readMode, err := normalizeSegmentReadMode(o.ReadMode)
	if err != nil {
		return StorxLogOptions{}, err
	}
	o.ReadMode = readMode
	return o, nil
}

func normalizeSegmentCompression(value SegmentCompression) (SegmentCompression, error) {
	normalized := SegmentCompression(strings.ToLower(strings.TrimSpace(string(value))))
	switch normalized {
	case "":
		return SegmentCompressionZstd, nil
	case SegmentCompressionNone:
		return SegmentCompressionNone, nil
	case SegmentCompressionZstd:
		return SegmentCompressionZstd, nil
	default:
		return "", E(CodeInvalidArgument, "unsupported segment compression %q", value)
	}
}

func normalizeSegmentReadMode(value SegmentReadMode) (SegmentReadMode, error) {
	normalized := SegmentReadMode(strings.ToLower(strings.TrimSpace(string(value))))
	switch normalized {
	case "":
		return SegmentReadModePread, nil
	case SegmentReadModePread:
		return SegmentReadModePread, nil
	case SegmentReadModeMmap:
		return SegmentReadModeMmap, nil
	default:
		return "", E(CodeInvalidArgument, "unsupported segment read mode %q", value)
	}
}
