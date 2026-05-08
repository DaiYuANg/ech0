package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/arcgolabs/dix"
)

func main() {
	cfg := parseFlags()
	if err := run(cfg); err != nil {
		exitWithError(err)
	}
}

func run(cfg benchConfig) error {
	if err := validateBenchConfig(cfg); err != nil {
		return err
	}
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app, err := newBenchmarkApp(rootCtx, cfg)
	if err != nil {
		return err
	}
	return runBenchmarkApp(rootCtx, app)
}

func runBenchmarkApp(ctx context.Context, app *dix.App) (err error) {
	rt, err := app.Start(ctx)
	if err != nil {
		return fmt.Errorf("start ech0bench app: %w", err)
	}
	defer func() {
		err = errors.Join(err, stopBenchmarkRuntime(ctx, rt))
	}()

	runner, err := dix.ResolveAs[*benchRunner](rt.Container())
	if err != nil {
		return fmt.Errorf("resolve ech0bench runner: %w", err)
	}
	return runner.Run(ctx)
}

type benchRunResult struct {
	counters        *benchCounters
	publish         latencySnapshot
	fetch           latencySnapshot
	activeConsumers uint32
	elapsed         time.Duration
}

func runWorkers(ctx context.Context, mq benchBroker, cfg benchConfig) benchRunResult {
	counters := &benchCounters{}
	publishLatencies := newLatencyRecorder(cfg.samples)
	fetchLatencies := newLatencyRecorder(cfg.samples)
	payload := benchmarkPayload(cfg.payloadBytes)
	started := time.Now()
	var workers sync.WaitGroup

	for id := range cfg.producers {
		producerID := id
		partition := producerID % cfg.partitions
		workers.Go(func() {
			runProducer(ctx, mq, cfg, producerID, partition, payload, counters, publishLatencies)
		})
	}
	activeConsumers := min(cfg.consumers, cfg.partitions)
	for partition := range activeConsumers {
		topicPartition := partition
		workers.Go(func() {
			runConsumer(ctx, mq, cfg, topicPartition, counters, fetchLatencies)
		})
	}

	workers.Wait()
	return benchRunResult{
		counters:        counters,
		publish:         publishLatencies.snapshot(),
		fetch:           fetchLatencies.snapshot(),
		activeConsumers: activeConsumers,
		elapsed:         time.Since(started),
	}
}

func stopBenchmarkRuntime(ctx context.Context, rt *dix.Runtime) error {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	if err := rt.Stop(stopCtx); err != nil {
		return fmt.Errorf("stop ech0bench app: %w", err)
	}
	return nil
}
