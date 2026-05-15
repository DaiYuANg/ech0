package broker

import (
	"strconv"

	"github.com/lyonbrown4d/ech0/store"
)

func lagRecordsFromOffsets(committedNextOffset uint64, offsets store.PartitionOffsetState) uint64 {
	if offsets.RetainedRecords == 0 || offsets.HighWatermark == nil {
		return 0
	}
	if committedNextOffset <= offsets.LogStartOffset {
		return offsets.RetainedRecords
	}
	if committedNextOffset > *offsets.HighWatermark {
		return 0
	}
	remaining := *offsets.HighWatermark - committedNextOffset + 1
	return min(remaining, offsets.RetainedRecords)
}

func displayUint64Ptr(value *uint64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatUint(*value, 10)
}
