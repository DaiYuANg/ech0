package broker

import (
	"context"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron/v2"
)

func registerRequestReplyCleanupJob(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	interval := durationFromSeconds(cfg.Broker.RequestReplyCleanupIntervalSecs, 30*time.Second)
	_, err := scheduler.NewJob(
		gocron.DurationJob(interval),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.CleanupRequestRepliesOnce(ctx)
			if err != nil {
				return err
			}
			logMoved(logger, "request reply cleanup removed cursors", result.RemovedCursors)
			return nil
		}),
		gocron.WithName("ech0.request_reply_cleanup"),
		gocron.WithTags("ech0", "request_reply"),
	)
	return wrapBroker("request_reply_cleanup_job_register_failed", err, "register request reply cleanup job")
}
