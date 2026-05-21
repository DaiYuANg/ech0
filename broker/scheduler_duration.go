package broker

import (
	"strconv"
	"time"
)

func durationFromSeconds(seconds uint64, fallback time.Duration) time.Duration {
	if seconds == 0 {
		return fallback
	}
	return boundedDuration(seconds, time.Second)
}

func durationFromMillis(milliseconds uint64) time.Duration {
	return boundedDuration(milliseconds, time.Millisecond)
}

func boundedDuration(value uint64, unit time.Duration) time.Duration {
	const maxDuration = time.Duration(1<<63 - 1)
	maxValue, err := durationMaxValue(maxDuration, unit)
	if err != nil {
		return maxDuration
	}
	if value > maxValue {
		return maxDuration
	}
	parsed, err := strconv.ParseInt(strconv.FormatUint(value, 10), 10, 64)
	if err != nil {
		return maxDuration
	}
	return time.Duration(parsed) * unit
}

func durationMaxValue(maxDuration, unit time.Duration) (uint64, error) {
	value, err := strconv.ParseUint(strconv.FormatInt(int64(maxDuration/unit), 10), 10, 64)
	if err != nil {
		return 0, wrapBroker("duration_max_value_failed", err, "parse max duration")
	}
	return value, nil
}
