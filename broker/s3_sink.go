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
	s3SinkDefaultInterval = 5 * time.Second
	s3SinkDefaultTimeout  = 5 * time.Second
	s3SinkDefaultRecords  = 100
	s3SinkDefaultConsumer = "__s3_sink"
	s3SinkPrincipal       = "s3-sink"
	s3SinkDefaultRegion   = "us-east-1"
)

type S3SinkResult = SinkResult

type s3SinkEnvelope struct {
	Sink      string                `json:"sink"`
	Consumer  string                `json:"consumer"`
	Topic     string                `json:"topic"`
	Partition uint32                `json:"partition"`
	Record    gatewayRecordResponse `json:"record"`
}

func registerS3SinkJobs(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	for index := range cfg.Broker.S3Sinks {
		sink := cfg.Broker.S3Sinks[index]
		if err := registerS3SinkJob(scheduler, broker, logger, sink); err != nil {
			return err
		}
	}
	return nil
}

func registerS3SinkJob(
	scheduler gocron.Scheduler,
	broker *Broker,
	logger *slog.Logger,
	sink S3SinkConfig,
) error {
	if err := validateS3SinkConfig(sink); err != nil {
		return err
	}
	name := s3SinkName(sink)
	_, err := scheduler.NewJob(
		gocron.DurationJob(durationFromSeconds(sink.IntervalSecs, s3SinkDefaultInterval)),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.ProcessS3SinkOnce(ctx, sink)
			if err != nil {
				return err
			}
			if result.Delivered > 0 && logger != nil {
				logger.Info("s3 sink wrote records", "name", name, "topic", sink.Topic, "partition", sink.Partition, "delivered", result.Delivered)
			}
			return nil
		}),
		gocron.WithName("ech0.s3_sink."+name),
		gocron.WithTags("ech0", "s3", "sink"),
	)
	return wrapBroker("s3_sink_job_register_failed", err, "register s3 sink job")
}

func (b *Broker) ProcessS3SinkOnce(ctx context.Context, sink S3SinkConfig) (S3SinkResult, error) {
	if err := validateS3SinkConfig(sink); err != nil {
		return S3SinkResult{}, err
	}
	consumer := s3SinkConsumer(sink)
	client, err := newHTTPSinkClient("s3", s3SinkTimeout(sink))
	if err != nil {
		return S3SinkResult{}, err
	}
	result, processErr := b.processRecordSinkOnce(ctx, recordSinkRun{
		Identity:   s3SinkIdentity(b.cfg, sink),
		Consumer:   consumer,
		Topic:      sink.Topic,
		Partition:  sink.Partition,
		MaxRecords: s3SinkMaxRecords(sink),
		Metadata:   "s3:" + s3SinkName(sink),
	}, func(ctx context.Context, record store.Record) error {
		return deliverS3SinkRecord(ctx, client, sink, consumer, record)
	})
	closeErr := closeHTTPSinkClient("s3", client)
	if processErr != nil {
		return result, processErr
	}
	return result, closeErr
}

func deliverS3SinkRecord(ctx context.Context, client httpSinkClient, sink S3SinkConfig, consumer string, record store.Record) error {
	payload, err := marshalJSON(s3SinkRecordEnvelope(sink, consumer, record))
	if err != nil {
		return err
	}
	objectURL := s3SinkObjectURL(sink, record)
	headers, err := signedS3SinkHeaders(ctx, sink, payload, objectURL, time.Now().UTC())
	if err != nil {
		return err
	}
	return deliverHTTPSink(ctx, client, httpSinkDelivery{
		Kind:    "s3",
		Name:    s3SinkName(sink),
		Method:  http.MethodPut,
		URL:     objectURL,
		Headers: headers,
		Payload: payload,
	})
}

func signedS3SinkHeaders(ctx context.Context, sink S3SinkConfig, payload []byte, objectURL string, now time.Time) (http.Header, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, objectURL, http.NoBody)
	if err != nil {
		return nil, wrapBroker("s3_sink_request_create_failed", err, "create s3 sink request")
	}
	request.Header = sinkHeaders("application/json", sink.Headers)
	if err := signS3SinkRequest(ctx, request, sink, payload, now); err != nil {
		return nil, err
	}
	return request.Header, nil
}

func s3SinkRecordEnvelope(sink S3SinkConfig, consumer string, record store.Record) s3SinkEnvelope {
	return s3SinkEnvelope{
		Sink:      s3SinkName(sink),
		Consumer:  consumer,
		Topic:     sink.Topic,
		Partition: sink.Partition,
		Record:    gatewayRecord(record),
	}
}

func validateS3SinkConfig(sink S3SinkConfig) error {
	if !validRequiredString(sink.Topic) || !validRequiredString(s3SinkEndpoint(sink)) || !validRequiredString(sink.Bucket) {
		return brokerStoreError(store.CodeInvalidArgument, "s3 sink requires topic, endpoint_url, and bucket")
	}
	parsed, err := url.ParseRequestURI(s3SinkEndpoint(sink))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return brokerStoreError(store.CodeInvalidArgument, "s3 sink %q has invalid endpoint_url", s3SinkName(sink))
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return brokerStoreError(store.CodeInvalidArgument, "s3 sink %q requires http or https endpoint_url", s3SinkName(sink))
	}
	if !validNonNegativeInt(sink.MaxRecords) {
		return brokerStoreError(store.CodeInvalidArgument, "s3 sink %q max_records cannot be negative", s3SinkName(sink))
	}
	return nil
}

func s3SinkIdentity(cfg Config, sink S3SinkConfig) Identity {
	return Identity{
		Tenant:    valueOr(sink.Tenant, cfg.Governance.DefaultTenant),
		Namespace: valueOr(sink.Namespace, cfg.Governance.DefaultNamespace),
		Principal: valueOr(sink.Principal, s3SinkPrincipal),
	}
}

func s3SinkName(sink S3SinkConfig) string {
	if strings.TrimSpace(sink.Name) != "" {
		return strings.TrimSpace(sink.Name)
	}
	return sink.Topic + "." + strconv.FormatUint(uint64(sink.Partition), 10)
}

func s3SinkConsumer(sink S3SinkConfig) string {
	if strings.TrimSpace(sink.Consumer) != "" {
		return strings.TrimSpace(sink.Consumer)
	}
	return s3SinkDefaultConsumer + "." + s3SinkName(sink)
}

func s3SinkEndpoint(sink S3SinkConfig) string {
	return strings.TrimRight(strings.TrimSpace(sink.EndpointURL), "/")
}

func s3SinkObjectURL(sink S3SinkConfig, record store.Record) string {
	return s3SinkEndpoint(sink) + "/" + url.PathEscape(strings.TrimSpace(sink.Bucket)) + "/" + s3SinkObjectKey(sink, record)
}

func s3SinkObjectKey(sink S3SinkConfig, record store.Record) string {
	parts := []string{s3SinkKeyComponent(sink.Topic), strconv.FormatUint(uint64(sink.Partition), 10), strconv.FormatUint(record.Offset, 10) + ".json"}
	prefix := strings.Trim(strings.TrimSpace(sink.Prefix), "/")
	if prefix != "" {
		parts = append([]string{s3SinkKeyComponent(prefix)}, parts...)
	}
	return strings.Join(parts, "/")
}

func s3SinkKeyComponent(value string) string {
	return strings.Trim(url.PathEscape(strings.Trim(value, "/")), "/")
}

func s3SinkRegion(sink S3SinkConfig) string {
	return valueOr(strings.TrimSpace(sink.Region), s3SinkDefaultRegion)
}

func s3SinkMaxRecords(sink S3SinkConfig) int {
	if sink.MaxRecords == 0 {
		return s3SinkDefaultRecords
	}
	return sink.MaxRecords
}

func s3SinkTimeout(sink S3SinkConfig) time.Duration {
	if sink.TimeoutMS == 0 {
		return s3SinkDefaultTimeout
	}
	return durationFromMillis(sink.TimeoutMS)
}
