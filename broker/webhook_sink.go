package broker

import (
	"bytes"
	"context"
	"errors"
	"io"
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
	client := &http.Client{Timeout: webhookSinkTimeout(sink)}
	return b.processRecordSinkOnce(ctx, recordSinkRun{
		Identity:   webhookSinkIdentity(b.cfg, sink),
		Consumer:   consumer,
		Topic:      sink.Topic,
		Partition:  sink.Partition,
		MaxRecords: webhookSinkMaxRecords(sink),
		Metadata:   "webhook:" + webhookSinkName(sink),
	}, func(ctx context.Context, record store.Record) error {
		return deliverWebhookSinkRecord(ctx, client, sink, consumer, record)
	})
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

func deliverWebhookSinkRecord(ctx context.Context, client *http.Client, sink WebhookSinkConfig, consumer string, record store.Record) (err error) {
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
	request, err := http.NewRequestWithContext(ctx, webhookSinkMethod(sink), webhookSinkURL(sink), bytes.NewReader(payload))
	if err != nil {
		return wrapBroker("webhook_sink_request_create_failed", err, "create webhook sink request")
	}
	request.Header.Set("Content-Type", "application/json")
	for index := range sink.Headers {
		header := sink.Headers[index]
		if strings.TrimSpace(header.Key) != "" {
			request.Header.Set(header.Key, header.Value)
		}
	}
	response, err := client.Do(request)
	if err != nil {
		return wrapBroker("webhook_sink_request_failed", err, "deliver webhook sink record")
	}
	defer func() {
		_, drainErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if err == nil {
			err = wrapBroker("webhook_sink_response_close_failed", errors.Join(drainErr, closeErr), "close webhook sink response")
		}
	}()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return brokerStoreError(store.CodeUnavailable, "webhook sink %q returned status %d", webhookSinkName(sink), response.StatusCode)
	}
	return nil
}

func validateWebhookSinkConfig(sink WebhookSinkConfig) error {
	if strings.TrimSpace(sink.Topic) == "" || webhookSinkURL(sink) == "" {
		return brokerStoreError(store.CodeInvalidArgument, "webhook sink requires topic and url")
	}
	parsed, err := url.ParseRequestURI(webhookSinkURL(sink))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return brokerStoreError(store.CodeInvalidArgument, "webhook sink %q has invalid url", webhookSinkName(sink))
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return brokerStoreError(store.CodeInvalidArgument, "webhook sink %q requires http or https url", webhookSinkName(sink))
	}
	if sink.MaxRecords < 0 {
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
