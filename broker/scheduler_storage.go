package broker

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron/v2"
)

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
