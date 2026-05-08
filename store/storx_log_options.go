package store

import (
	"log/slog"
	"strings"

	"github.com/arcgolabs/storx/observer"
)

type SegmentCompression string

type StorxObserver = observer.Observer

const (
	SegmentCompressionNone SegmentCompression = "none"
	SegmentCompressionZstd SegmentCompression = "zstd"
)

type StorxLogOptions struct {
	Compression SegmentCompression
	Logger      *slog.Logger
	Observers   []observer.Observer
}

func (o StorxLogOptions) normalize() (StorxLogOptions, error) {
	compression, err := normalizeSegmentCompression(o.Compression)
	if err != nil {
		return StorxLogOptions{}, err
	}
	o.Compression = compression
	o.Observers = observer.Clone(o.Observers)
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
