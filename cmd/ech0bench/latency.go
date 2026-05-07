package main

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

type latencyRecorder struct {
	mu      sync.Mutex
	limit   int
	samples []time.Duration
	total   time.Duration
	max     time.Duration
	count   int64
}

type latencySnapshot struct {
	count   int64
	total   time.Duration
	max     time.Duration
	samples []time.Duration
}

func newLatencyRecorder(limit int) *latencyRecorder {
	return &latencyRecorder{limit: limit, samples: make([]time.Duration, 0, min(limit, 4096))}
}

func (r *latencyRecorder) record(value time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	r.total += value
	if value > r.max {
		r.max = value
	}
	if len(r.samples) < r.limit {
		r.samples = append(r.samples, value)
	}
}

func (r *latencyRecorder) snapshot() latencySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	samples := append([]time.Duration(nil), r.samples...)
	slices.Sort(samples)
	return latencySnapshot{count: r.count, total: r.total, max: r.max, samples: samples}
}

func (s latencySnapshot) avg() time.Duration {
	if s.count == 0 {
		return 0
	}
	return s.total / time.Duration(s.count)
}

func (s latencySnapshot) percentile(percent float64) time.Duration {
	if len(s.samples) == 0 {
		return 0
	}
	index := int((percent / 100) * float64(len(s.samples)-1))
	return s.samples[index]
}

func formatDurationASCII(duration time.Duration) string {
	if duration < time.Microsecond {
		return fmt.Sprintf("%dns", duration.Nanoseconds())
	}
	if duration < time.Millisecond {
		return fmt.Sprintf("%.3fus", float64(duration.Nanoseconds())/float64(time.Microsecond))
	}
	if duration < time.Second {
		return fmt.Sprintf("%.3fms", float64(duration.Nanoseconds())/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.3fs", duration.Seconds())
}
