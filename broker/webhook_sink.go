package broker

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	webhookSinkDefaultInterval = 5 * time.Second
	webhookSinkDefaultTimeout  = 5 * time.Second
	webhookSinkDefaultRecords  = 100
	webhookSinkDefaultConsumer = "__webhook_sink"
	webhookSinkPrincipal       = "webhook-sink"
)

type WebhookSinkResult = SinkResult

type webhookSinkEnvelope struct {
	Sink      string                `json:"sink"`
	Consumer  string                `json:"consumer"`
	Topic     string                `json:"topic"`
	Partition uint32                `json:"partition"`
	Record    gatewayRecordResponse `json:"record"`
}

func registerWebhookSinkJobs(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	for index := range cfg.Broker.WebhookSinks {
		sink := cfg.Broker.WebhookSinks[index]
		if err := registerWebhookSinkJob(scheduler, broker, logger, sink); err != nil {
			return err
		}
	}
	return nil
}

func registerWebhookSinkJob(
	scheduler gocron.Scheduler,
	broker *Broker,
	logger *slog.Logger,
	sink WebhookSinkConfig,
) error {
	if err := validateWebhookSinkConfig(sink); err != nil {
		return err
	}
	name := webhookSinkName(sink)
	_, err := scheduler.NewJob(
		gocron.DurationJob(durationFromSeconds(sink.IntervalSecs, webhookSinkDefaultInterval)),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.ProcessWebhookSinkOnce(ctx, sink)
			if err != nil {
				return err
			}
			if result.Delivered > 0 && logger != nil {
				logger.Info("webhook sink delivered records", "name", name, "topic", sink.Topic, "partition", sink.Partition, "delivered", result.Delivered)
			}
			return nil
		}),
		gocron.WithName("ech0.webhook_sink."+name),
		gocron.WithTags("ech0", "webhook", "sink"),
	)
	return wrapBroker("webhook_sink_job_register_failed", err, "register webhook sink job")
}

func (b *Broker) ProcessWebhookSinkOnce(ctx context.Context, sink WebhookSinkConfig) (WebhookSinkResult, error) {
	if err := validateWebhookSinkConfig(sink); err != nil {
		return WebhookSinkResult{}, err
	}
	consumer := webhookSinkConsumer(sink)
	client, err := newHTTPSinkClient("webhook", webhookSinkTimeout(sink))
	if err != nil {
		return WebhookSinkResult{}, err
	}
	result, processErr := b.processRecordSinkOnce(ctx, recordSinkRun{
		Identity:   webhookSinkIdentity(b.cfg, sink),
		Consumer:   consumer,
		Topic:      sink.Topic,
		Partition:  sink.Partition,
		MaxRecords: webhookSinkMaxRecords(sink),
		Metadata:   "webhook:" + webhookSinkName(sink),
	}, func(ctx context.Context, record store.Record) error {
		return deliverWebhookSinkRecord(ctx, client, sink, consumer, record)
	})
	closeErr := closeHTTPSinkClient("webhook", client)
	if processErr != nil {
		return result, processErr
	}
	return result, closeErr
}

func (r *ScheduledRuntime) ScheduleWebhookSink(ctx context.Context, sink WebhookSinkConfig) error {
	if err := ctx.Err(); err != nil {
		return wrapBroker("webhook_sink_context_done", err, "schedule webhook sink")
	}
	if r == nil || r.scheduler == nil {
		return brokerStoreError(store.CodeInvalidArgument, "scheduled runtime is not enabled")
	}
	return registerWebhookSinkJob(r.scheduler, r.broker, r.logger, sink)
}

func deliverWebhookSinkRecord(ctx context.Context, client httpSinkClient, sink WebhookSinkConfig, consumer string, record store.Record) error {
	payload, err := marshalJSON(webhookSinkEnvelope{
		Sink:      webhookSinkName(sink),
		Consumer:  consumer,
		Topic:     sink.Topic,
		Partition: sink.Partition,
		Record:    gatewayRecord(record),
	})
	if err != nil {
		return err
	}
	return deliverHTTPSink(ctx, client, httpSinkDelivery{
		Kind:    "webhook",
		Name:    webhookSinkName(sink),
		Method:  webhookSinkMethod(sink),
		URL:     webhookSinkURL(sink),
		Headers: sinkHeaders("application/json", sink.Headers),
		Payload: payload,
	})
}

func validateWebhookSinkConfig(sink WebhookSinkConfig) error {
	if !validRequiredString(sink.Topic) || !validRequiredString(webhookSinkURL(sink)) {
		return brokerStoreError(store.CodeInvalidArgument, "webhook sink requires topic and url")
	}
	parsed, err := url.ParseRequestURI(webhookSinkURL(sink))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return brokerStoreError(store.CodeInvalidArgument, "webhook sink %q has invalid url", webhookSinkName(sink))
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return brokerStoreError(store.CodeInvalidArgument, "webhook sink %q requires http or https url", webhookSinkName(sink))
	}
	if !validNonNegativeInt(sink.MaxRecords) {
		return brokerStoreError(store.CodeInvalidArgument, "webhook sink %q max_records cannot be negative", webhookSinkName(sink))
	}
	return nil
}

func webhookSinkIdentity(cfg Config, sink WebhookSinkConfig) Identity {
	return Identity{
		Tenant:    valueOr(sink.Tenant, cfg.Governance.DefaultTenant),
		Namespace: valueOr(sink.Namespace, cfg.Governance.DefaultNamespace),
		Principal: valueOr(sink.Principal, webhookSinkPrincipal),
	}
}

func webhookSinkName(sink WebhookSinkConfig) string {
	if strings.TrimSpace(sink.Name) != "" {
		return strings.TrimSpace(sink.Name)
	}
	return sink.Topic + "." + webhookSinkPartitionString(sink.Partition)
}

func webhookSinkConsumer(sink WebhookSinkConfig) string {
	if strings.TrimSpace(sink.Consumer) != "" {
		return strings.TrimSpace(sink.Consumer)
	}
	return webhookSinkDefaultConsumer + "." + webhookSinkName(sink)
}

func webhookSinkMethod(sink WebhookSinkConfig) string {
	method := strings.ToUpper(strings.TrimSpace(sink.Method))
	if method == "" {
		return http.MethodPost
	}
	return method
}

func webhookSinkURL(sink WebhookSinkConfig) string {
	return strings.TrimSpace(sink.URL)
}

func webhookSinkMaxRecords(sink WebhookSinkConfig) int {
	if sink.MaxRecords == 0 {
		return webhookSinkDefaultRecords
	}
	return sink.MaxRecords
}

func webhookSinkTimeout(sink WebhookSinkConfig) time.Duration {
	if sink.TimeoutMS == 0 {
		return webhookSinkDefaultTimeout
	}
	return durationFromMillis(sink.TimeoutMS)
}

func webhookSinkPartitionString(partition uint32) string {
	return strconv.FormatUint(uint64(partition), 10)
}
