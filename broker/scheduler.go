package broker

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/go-co-op/gocron/v2"
	"github.com/lyonbrown4d/ech0/store"
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
	if !scheduledRuntimeEnabled(cfg) {
		return runtime, nil
	}
	scheduler, err := newScheduledRuntimeScheduler(broker)
	if err != nil {
		return nil, err
	}
	runtime.scheduler = scheduler
	if err := registerScheduledJobs(scheduler, cfg, broker, logger); err != nil {
		return nil, err
	}
	return runtime, nil
}

type scheduledJobRegistration struct {
	enabled  bool
	code     string
	message  string
	register func(gocron.Scheduler, Config, *Broker, *slog.Logger) error
}

func scheduledRuntimeEnabled(cfg Config) bool {
	return cfg.Broker.DelaySchedulerEnabled ||
		cfg.Broker.RetryWorkerEnabled ||
		cfg.Broker.TransactionCleanupEnabled ||
		cfg.Broker.RequestReplyCleanupEnabled ||
		len(cfg.Broker.CronSchedules) > 0 ||
		cfg.Storage.RetentionCleanupEnabled ||
		cfg.Storage.CompactionCleanupEnabled
}

func newScheduledRuntimeScheduler(broker *Broker) (gocron.Scheduler, error) {
	scheduler, err := gocron.NewScheduler(
		gocron.WithDistributedElector(raftElector{broker: broker}),
		gocron.WithGlobalJobOptions(gocron.WithSingletonMode(gocron.LimitModeReschedule)),
		gocron.WithStopTimeout(10*time.Second),
	)
	if err != nil {
		return nil, wrapBroker("scheduler_create_failed", err, "create scheduler")
	}
	return scheduler, nil
}

func registerScheduledJobs(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	for _, job := range scheduledJobRegistrations(cfg) {
		if !job.enabled {
			continue
		}
		if err := job.register(scheduler, cfg, broker, logger); err != nil {
			return wrapBroker(job.code, err, "%s", job.message)
		}
	}
	return nil
}

func scheduledJobRegistrations(cfg Config) []scheduledJobRegistration {
	return collectionlist.NewList(
		scheduledJobRegistration{
			enabled:  cfg.Broker.DelaySchedulerEnabled,
			code:     "delay_job_create_failed",
			message:  "create delay scheduler job",
			register: registerDelayJob,
		},
		scheduledJobRegistration{
			enabled:  cfg.Broker.RetryWorkerEnabled,
			code:     "retry_job_create_failed",
			message:  "create retry worker job",
			register: registerRetryJob,
		},
		scheduledJobRegistration{
			enabled:  cfg.Broker.TransactionCleanupEnabled,
			code:     "transaction_cleanup_job_create_failed",
			message:  "create transaction cleanup job",
			register: registerTransactionCleanupJob,
		},
		scheduledJobRegistration{
			enabled:  cfg.Broker.RequestReplyCleanupEnabled,
			code:     "request_reply_cleanup_job_create_failed",
			message:  "create request reply cleanup job",
			register: registerRequestReplyCleanupJob,
		},
		scheduledJobRegistration{
			enabled:  len(cfg.Broker.CronSchedules) > 0,
			code:     "cron_message_job_create_failed",
			message:  "create cron message jobs",
			register: registerCronMessageJobs,
		},
		scheduledJobRegistration{
			enabled:  cfg.Storage.RetentionCleanupEnabled,
			code:     "retention_cleanup_job_create_failed",
			message:  "create retention cleanup job",
			register: registerRetentionCleanupJob,
		},
		scheduledJobRegistration{
			enabled:  cfg.Storage.CompactionCleanupEnabled,
			code:     "compaction_cleanup_job_create_failed",
			message:  "create compaction cleanup job",
			register: registerCompactionCleanupJob,
		},
	).Values()
}

func registerDelayJob(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	interval := durationFromSeconds(cfg.Broker.DelaySchedulerIntervalSecs, time.Second)
	_, err := scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(func(ctx context.Context) error {
			moved, err := broker.ProcessDueDelayedOnce(ctx, cfg.Broker.DelaySchedulerConsumerPrefix, cfg.Broker.DelaySchedulerMaxRecords)
			if err != nil {
				return err
			}
			logMoved(logger, "delay scheduler forwarded records", moved)
			return nil
		}),
		gocron.WithName("ech0.delay_scheduler"),
		gocron.WithTags("ech0", "delay"),
	)
	return wrapBroker("delay_job_register_failed", err, "register delay scheduler job")
}

func registerRetryJob(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	interval := durationFromSeconds(cfg.Broker.RetryWorkerIntervalSecs, 5*time.Second)
	_, err := scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(func(ctx context.Context) error {
			moved, err := broker.ProcessRetryTopicsOnce(ctx, cfg.Broker.RetryWorkerConsumerPrefix, cfg.Broker.RetryWorkerMaxRecords)
			if err != nil {
				if store.ErrorCode(err) == store.CodeTopicNotFound {
					return nil
				}
				return err
			}
			logMoved(logger, "retry worker moved records", moved)
			return nil
		}),
		gocron.WithName("ech0.retry_worker"),
		gocron.WithTags("ech0", "retry"),
	)
	return wrapBroker("retry_job_register_failed", err, "register retry worker job")
}

func registerTransactionCleanupJob(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	interval := durationFromSeconds(cfg.Broker.TransactionCleanupIntervalSecs, 5*time.Second)
	_, err := scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.ExpireTransactionsOnce(ctx)
			if err != nil {
				return err
			}
			logMoved(logger, "transaction cleanup expired transactions", result.Expired)
			return nil
		}),
		gocron.WithName("ech0.transaction_cleanup"),
		gocron.WithTags("ech0", "transaction"),
	)
	return wrapBroker("transaction_cleanup_job_register_failed", err, "register transaction cleanup job")
}

func registerRetentionCleanupJob(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	interval := durationFromSeconds(cfg.Storage.RetentionCleanupIntervalSecs, 30*time.Second)
	_, err := scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.EnforceRetentionOnce(ctx)
			if err != nil {
				return err
			}
			logMoved(logger, "retention cleanup removed records", result.RemovedRecords)
			return nil
		}),
		gocron.WithName("ech0.retention_cleanup"),
		gocron.WithTags("ech0", "storage", "retention"),
	)
	return wrapBroker("retention_cleanup_job_register_failed", err, "register retention cleanup job")
}

func registerCompactionCleanupJob(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	interval := durationFromSeconds(cfg.Storage.CompactionCleanupIntervalSecs, 60*time.Second)
	_, err := scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.CompactOnce(ctx)
			if err != nil {
				return err
			}
			if result.RemovedRecords > 0 && logger != nil {
				logger.Info("compaction cleanup removed records", "removed", result.RemovedRecords, "partitions", result.CompactedPartitions)
			}
			return nil
		}),
		gocron.WithName("ech0.compaction_cleanup"),
		gocron.WithTags("ech0", "storage", "compaction"),
	)
	return wrapBroker("compaction_cleanup_job_register_failed", err, "register compaction cleanup job")
}

func (r *ScheduledRuntime) Start(ctx context.Context) error {
	_ = ctx
	if r == nil || r.scheduler == nil {
		return nil
	}
	r.scheduler.Start()
	if r.logger != nil {
		r.logger.Info("scheduled runtime started", "runtime", "gocron", "distributed_elector", "dragonboat")
	}
	return nil
}

func (r *ScheduledRuntime) Stop(ctx context.Context) error {
	if r == nil || r.scheduler == nil {
		return nil
	}
	return wrapBroker("scheduler_shutdown_failed", r.scheduler.ShutdownWithContext(ctx), "shutdown scheduler")
}

type raftElector struct {
	broker *Broker
}

func (e raftElector) IsLeader(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return wrapBroker("scheduler_election_context_done", err, "check scheduled job leadership")
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
	health := b.RuntimeHealth()
	return health.Raft != nil && health.Raft.LocalIsLeader
}

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
