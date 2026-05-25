package client

import (
	"strconv"
	"time"
)

func durationMillis(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	millis := duration / time.Millisecond
	if millis <= 0 {
		return 1
	}
	return int64ToUint64(int64(millis))
}

func unixMillis(timestamp time.Time) uint64 {
	millis := timestamp.UnixMilli()
	if millis <= 0 {
		return 0
	}
	return int64ToUint64(millis)
}

func nonNegativeIntToUint64(value int) uint64 {
	if value <= 0 {
		return 0
	}
	parsed, err := strconv.ParseUint(strconv.Itoa(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func int64ToUint64(value int64) uint64 {
	if value <= 0 {
		return 0
	}
	parsed, err := strconv.ParseUint(strconv.FormatInt(value, 10), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}
