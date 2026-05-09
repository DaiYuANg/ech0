package main

import (
	"fmt"
	"sync"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const latencyHighestTrackable = int64(time.Hour)

type latencyRecorder struct {
	mu    sync.Mutex
	hist  *hdrhistogram.Histogram
	total time.Duration
	max   time.Duration
	count int64
}

type latencySnapshot struct {
	count int64
	total time.Duration
	max   time.Duration
	hist  *hdrhistogram.Histogram
}

func newLatencyRecorder(limit int) *latencyRecorder {
	_ = limit
	return &latencyRecorder{hist: hdrhistogram.New(1, latencyHighestTrackable, 3)}
}

func (r *latencyRecorder) record(value time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.count++
	r.total += value
	if value > r.max {
		r.max = value
	}
	recorded := min(max(value.Nanoseconds(), int64(1)), latencyHighestTrackable)
	if err := r.hist.RecordValue(recorded); err != nil {
		panic(err)
	}
}

func (r *latencyRecorder) snapshot() latencySnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	return latencySnapshot{
		count: r.count,
		total: r.total,
		max:   r.max,
		hist:  hdrhistogram.Import(r.hist.Export()),
	}
}

func (s latencySnapshot) avg() time.Duration {
	if s.count == 0 {
		return 0
	}
	return s.total / time.Duration(s.count)
}

func (s latencySnapshot) percentile(percent float64) time.Duration {
	if s.hist == nil || s.hist.TotalCount() == 0 {
		return 0
	}
	return time.Duration(s.hist.ValueAtPercentile(percent))
}

func (s latencySnapshot) recorded() int64 {
	if s.hist == nil {
		return 0
	}
	return s.hist.TotalCount()
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
