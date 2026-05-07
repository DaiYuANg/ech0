package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

type benchConfig struct {
	dataDir         string
	topic           string
	partitions      uint32
	producers       uint32
	consumers       uint32
	activeConsumers uint32
	duration        time.Duration
	payloadBytes    int
	fetchBatch      int
	pollIdle        time.Duration
	samples         int
}

func parseFlags() benchConfig {
	cfg := benchConfig{}
	flag.StringVar(&cfg.dataDir, "data-dir", "", "data directory; defaults to a temporary directory")
	flag.StringVar(&cfg.topic, "topic", "ech0-bench", "benchmark topic name")
	uint32Var(&cfg.partitions, "partitions", 4, "topic partition count")
	uint32Var(&cfg.producers, "producers", 4, "producer goroutines")
	uint32Var(&cfg.consumers, "consumers", 4, "consumer goroutines, capped to partition count")
	flag.DurationVar(&cfg.duration, "duration", 30*time.Second, "stress test duration")
	flag.IntVar(&cfg.payloadBytes, "payload-bytes", 1024, "message payload size")
	flag.IntVar(&cfg.fetchBatch, "fetch-batch", 128, "max records per fetch")
	flag.DurationVar(&cfg.pollIdle, "poll-idle", time.Millisecond, "sleep duration after an empty fetch")
	flag.IntVar(&cfg.samples, "samples", 200000, "max latency samples kept in memory")
	flag.Parse()
	return cfg
}

func validateBenchConfig(cfg benchConfig) error {
	if cfg.partitions == 0 {
		return errors.New("partitions must be greater than zero")
	}
	if cfg.producers == 0 {
		return errors.New("producers must be greater than zero")
	}
	if cfg.consumers == 0 {
		return errors.New("consumers must be greater than zero")
	}
	if cfg.duration <= 0 {
		return errors.New("duration must be greater than zero")
	}
	if cfg.payloadBytes <= 0 {
		return errors.New("payload-bytes must be greater than zero")
	}
	if cfg.fetchBatch <= 0 {
		return errors.New("fetch-batch must be greater than zero")
	}
	if cfg.samples <= 0 {
		return errors.New("samples must be greater than zero")
	}
	return nil
}

type uint32Flag struct {
	value *uint32
}

func uint32Var(target *uint32, name string, value uint32, usage string) {
	*target = value
	flag.Var(uint32Flag{value: target}, name, usage)
}

func (f uint32Flag) String() string {
	if f.value == nil {
		return ""
	}
	return strconv.FormatUint(uint64(*f.value), 10)
}

func (f uint32Flag) Set(input string) error {
	var parsed uint32
	if _, err := fmt.Sscan(input, &parsed); err != nil {
		return fmt.Errorf("parse uint32 flag %q: %w", input, err)
	}
	*f.value = parsed
	return nil
}

func benchmarkDataDir(configured string) (string, func(), error) {
	if configured != "" {
		return configured, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "ech0bench-*")
	if err != nil {
		return "", nil, fmt.Errorf("create temporary data directory: %w", err)
	}
	return dir, func() {
		if err := os.RemoveAll(dir); err != nil {
			writeStderr("remove temporary data directory: %v\n", err)
		}
	}, nil
}
