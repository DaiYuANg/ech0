package broker

import (
	"context"
	"log/slog"

	"github.com/go-co-op/gocron/v2"
	"github.com/lyonbrown4d/ech0/store"
)

func registerCronMessageJobs(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	for index := range cfg.Broker.CronSchedules {
		schedule := cfg.Broker.CronSchedules[index]
		if err := registerCronMessageJob(scheduler, cfg, broker, logger, schedule); err != nil {
			return err
		}
	}
	return nil
}

func registerCronMessageJob(
	scheduler gocron.Scheduler,
	cfg Config,
	broker *Broker,
	logger *slog.Logger,
	schedule CronMessageConfig,
) error {
	if schedule.Topic == "" || schedule.Cron == "" {
		return brokerStoreError(store.CodeInvalidArgument, "cron message schedule requires topic and cron")
	}
	name := valueOr(schedule.Name, "ech0.cron_message."+schedule.Topic)
	_, err := scheduler.NewJob(
		gocron.CronJob(schedule.Cron, schedule.WithSeconds),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := publishCronMessage(ctx, cfg, broker, schedule)
			if err != nil {
				return err
			}
			if logger != nil {
				logger.Info("cron message published", "name", name, "topic", schedule.Topic, "partition", result.Partition, "offset", result.Record.Offset)
			}
			return nil
		}),
		gocron.WithName(name),
		gocron.WithTags("ech0", "cron", "message"),
	)
	return wrapBroker("cron_message_job_register_failed", err, "register cron message job")
}

func publishCronMessage(ctx context.Context, cfg Config, broker *Broker, schedule CronMessageConfig) (ProduceResult, error) {
	identity := Identity{
		Tenant:    valueOr(schedule.Tenant, cfg.Governance.DefaultTenant),
		Namespace: valueOr(schedule.Namespace, cfg.Governance.DefaultNamespace),
		Principal: valueOr(schedule.Principal, "scheduler"),
	}
	record := store.NewRecordAppend([]byte(schedule.Payload))
	for index := range schedule.Headers {
		header := schedule.Headers[index]
		if header.Key != "" {
			record.Headers = append(record.Headers, store.RecordHeader{Key: header.Key, Value: []byte(header.Value)})
		}
	}
	return broker.PublishRecord(WithIdentity(ctx, identity), schedule.Topic, PublishPartitioning{
		Mode:      PartitionExplicit,
		Partition: schedule.Partition,
	}, record)
}

func (r *ScheduledRuntime) ScheduleCronMessage(ctx context.Context, schedule CronMessageConfig) error {
	if err := ctx.Err(); err != nil {
		return wrapBroker("cron_message_context_done", err, "schedule cron message")
	}
	if r == nil || r.scheduler == nil {
		return brokerStoreError(store.CodeInvalidArgument, "scheduled runtime is not enabled")
	}
	return registerCronMessageJob(r.scheduler, r.cfg, r.broker, r.logger, schedule)
}
