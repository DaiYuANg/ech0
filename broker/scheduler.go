package broker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/DaiYuANg/ech0/store"
	"github.com/go-co-op/gocron/v2"
)

var errScheduledRuntimeNotLeader = errors.New("ech0 scheduled runtime is not raft leader")

type ScheduledRuntime struct {
	cfg       Config
	broker    *Broker
	logger    *slog.Logger
	scheduler gocron.Scheduler
}

func NewScheduledRuntime(cfg Config, broker *Broker, logger *slog.Logger) (*ScheduledRuntime, error) {
	runtime := &ScheduledRuntime{cfg: cfg, broker: broker, logger: logger}
	if !cfg.Broker.DelaySchedulerEnabled && !cfg.Broker.RetryWorkerEnabled {
		return runtime, nil
	}
	scheduler, err := gocron.NewScheduler(
		gocron.WithDistributedElector(raftElector{broker: broker}),
		gocron.WithGlobalJobOptions(gocron.WithSingletonMode(gocron.LimitModeReschedule)),
		gocron.WithStopTimeout(10*time.Second),
	)
	if err != nil {
		return nil, err
	}
	runtime.scheduler = scheduler

	if cfg.Broker.DelaySchedulerEnabled {
		interval := durationFromSeconds(cfg.Broker.DelaySchedulerIntervalSecs, time.Second)
		if _, err := scheduler.NewJob(
			gocron.DurationJob(interval),
			gocron.NewTask(func(ctx context.Context) error {
				moved, err := broker.ProcessDueDelayedOnce(ctx, cfg.Broker.DelaySchedulerConsumerPrefix, cfg.Broker.DelaySchedulerMaxRecords)
				if err != nil {
					return err
				}
				if moved > 0 && logger != nil {
					logger.Info("delay scheduler forwarded records", "moved", moved)
				}
				return nil
			}),
			gocron.WithName("ech0.delay_scheduler"),
			gocron.WithTags("ech0", "delay"),
		); err != nil {
			return nil, err
		}
	}

	if cfg.Broker.RetryWorkerEnabled {
		interval := durationFromSeconds(cfg.Broker.RetryWorkerIntervalSecs, 5*time.Second)
		if _, err := scheduler.NewJob(
			gocron.DurationJob(interval),
			gocron.NewTask(func(ctx context.Context) error {
				moved, err := broker.ProcessRetryTopicsOnce(ctx, cfg.Broker.RetryWorkerConsumerPrefix, cfg.Broker.RetryWorkerMaxRecords)
				if err != nil {
					if store.ErrorCode(err) == store.CodeTopicNotFound {
						return nil
					}
					return err
				}
				if moved > 0 && logger != nil {
					logger.Info("retry worker moved records", "moved", moved)
				}
				return nil
			}),
			gocron.WithName("ech0.retry_worker"),
			gocron.WithTags("ech0", "retry"),
		); err != nil {
			return nil, err
		}
	}
	return runtime, nil
}

func (r *ScheduledRuntime) Start(ctx context.Context) error {
	_ = ctx
	if r == nil || r.scheduler == nil {
		return nil
	}
	r.scheduler.Start()
	if r.logger != nil {
		r.logger.Info("scheduled runtime started", "runtime", "gocron", "raft_elector", r.cfg.Raft.Enabled)
	}
	return nil
}

func (r *ScheduledRuntime) Stop(ctx context.Context) error {
	if r == nil || r.scheduler == nil {
		return nil
	}
	return r.scheduler.ShutdownWithContext(ctx)
}

type raftElector struct {
	broker *Broker
}

func (e raftElector) IsLeader(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e.broker == nil || e.broker.canRunScheduledJobs() {
		return nil
	}
	return errScheduledRuntimeNotLeader
}

func (b *Broker) canRunScheduledJobs() bool {
	if b == nil {
		return false
	}
	if !b.cfg.Raft.Enabled {
		return true
	}
	health := b.RuntimeHealth()
	return health.Raft != nil && health.Raft.LocalIsLeader
}

func durationFromSeconds(seconds uint64, fallback time.Duration) time.Duration {
	if seconds == 0 {
		return fallback
	}
	return time.Duration(seconds) * time.Second
}
