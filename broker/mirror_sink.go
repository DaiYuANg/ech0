package broker

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/go-co-op/gocron/v2"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	mirrorSinkDefaultInterval = 5 * time.Second
	mirrorSinkDefaultTimeout  = 5 * time.Second
	mirrorSinkDefaultRecords  = 100
	mirrorSinkDefaultConsumer = "__mirror_sink"
	mirrorSinkPrincipal       = "mirror-sink"
)

type MirrorSinkResult struct {
	Delivered           int
	CommittedNextOffset *uint64
}

type mirrorProduceRequest struct {
	Payload       string          `json:"payload,omitempty"`
	PayloadBase64 string          `json:"payload_base64,omitempty"`
	KeyBase64     string          `json:"key_base64,omitempty"`
	Headers       []gatewayHeader `json:"headers,omitempty"`
	RoutingKey    string          `json:"routing_key,omitempty"`
	Partition     uint32          `json:"partition"`
	Tombstone     bool            `json:"tombstone,omitempty"`
	ExpiresAtMS   *uint64         `json:"expires_at_ms,omitempty"`
}

func registerMirrorSinkJobs(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	for index := range cfg.Broker.MirrorSinks {
		sink := cfg.Broker.MirrorSinks[index]
		if err := registerMirrorSinkJob(scheduler, broker, logger, sink); err != nil {
			return err
		}
	}
	return nil
}

func registerMirrorSinkJob(
	scheduler gocron.Scheduler,
	broker *Broker,
	logger *slog.Logger,
	sink MirrorSinkConfig,
) error {
	if err := validateMirrorSinkConfig(sink); err != nil {
		return err
	}
	name := mirrorSinkName(sink)
	_, err := scheduler.NewJob(
		gocron.DurationJob(durationFromSeconds(sink.IntervalSecs, mirrorSinkDefaultInterval)),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.ProcessMirrorSinkOnce(ctx, sink)
			if err != nil {
				return err
			}
			if result.Delivered > 0 && logger != nil {
				logger.Info("mirror sink replicated records", "name", name, "topic", sink.Topic, "partition", sink.Partition, "delivered", result.Delivered)
			}
			return nil
		}),
		gocron.WithName("ech0.mirror_sink."+name),
		gocron.WithTags("ech0", "mirror", "sink"),
	)
	return wrapBroker("mirror_sink_job_register_failed", err, "register mirror sink job")
}

func (b *Broker) ProcessMirrorSinkOnce(ctx context.Context, sink MirrorSinkConfig) (result MirrorSinkResult, err error) {
	if validateErr := validateMirrorSinkConfig(sink); validateErr != nil {
		return MirrorSinkResult{}, validateErr
	}
	identity := mirrorSinkIdentity(b.cfg, sink)
	runCtx := WithIdentity(ctx, identity)
	consumer := mirrorSinkConsumer(sink)
	poll, err := b.FetchWithIsolation(runCtx, consumer, sink.Topic, sink.Partition, nil, mirrorSinkMaxRecords(sink), FetchIsolationReadCommitted)
	if err != nil {
		return MirrorSinkResult{}, err
	}
	client, err := newHTTPSinkClient("mirror", mirrorSinkTimeout(sink))
	if err != nil {
		return MirrorSinkResult{}, err
	}
	defer func() {
		closeErr := closeHTTPSinkClient("mirror", client)
		if err == nil {
			err = closeErr
		}
	}()
	for index := range poll.Records {
		record := poll.Records[index]
		if err := deliverMirrorSinkRecord(ctx, client, sink, record); err != nil {
			return result, err
		}
		nextOffset := record.Offset + 1
		if err := b.CommitOffsetWithMetadata(runCtx, consumer, sink.Topic, sink.Partition, nextOffset, "mirror:"+mirrorSinkName(sink)); err != nil {
			return result, err
		}
		result.Delivered++
		result.CommittedNextOffset = &nextOffset
	}
	return result, nil
}

func deliverMirrorSinkRecord(ctx context.Context, client httpSinkClient, sink MirrorSinkConfig, record store.Record) error {
	payload, err := marshalJSON(mirrorProduceRecord(sink, record))
	if err != nil {
		return err
	}
	return deliverHTTPSink(ctx, client, httpSinkDelivery{
		Kind:    "mirror",
		Name:    mirrorSinkName(sink),
		Method:  http.MethodPost,
		URL:     mirrorSinkProduceURL(sink),
		Headers: mirrorSinkHeaders(sink),
		Payload: payload,
	})
}

func mirrorSinkHeaders(sink MirrorSinkConfig) http.Header {
	headers := sinkHeaders("application/json", sink.Headers)
	if strings.TrimSpace(sink.AuthToken) != "" {
		headers.Set("Authorization", "Bearer "+strings.TrimSpace(sink.AuthToken))
	}
	setMirrorIdentityHeader(headers, "X-Ech0-Tenant", sink.TargetTenant)
	setMirrorIdentityHeader(headers, "X-Ech0-Namespace", sink.TargetNamespace)
	setMirrorIdentityHeader(headers, "X-Ech0-Principal", sink.TargetPrincipal)
	return headers
}

func setMirrorIdentityHeader(headers http.Header, key, value string) {
	if strings.TrimSpace(value) != "" {
		headers.Set(key, strings.TrimSpace(value))
	}
}

func mirrorProduceRecord(sink MirrorSinkConfig, record store.Record) mirrorProduceRequest {
	return mirrorProduceRequest{
		Payload:       gatewayUTF8(record.Payload),
		PayloadBase64: gatewayRecord(record).PayloadBase64,
		KeyBase64:     gatewayRecord(record).KeyBase64,
		Headers:       mirrorRecordHeaders(sink, record),
		RoutingKey:    recordRoutingKey(record),
		Partition:     sink.Partition,
		Tombstone:     record.IsTombstone(),
		ExpiresAtMS:   cloneUint64Ptr(record.ExpiresAtMS),
	}
}

func mirrorRecordHeaders(sink MirrorSinkConfig, record store.Record) []gatewayHeader {
	headers := collectionlist.NewList(gatewayHeadersFromStore(record.Headers)...)
	headers.Add(gatewayHeader{Key: "x-ech0-mirror-source-topic", Value: sink.Topic})
	headers.Add(gatewayHeader{Key: "x-ech0-mirror-source-partition", Value: strconv.FormatUint(uint64(sink.Partition), 10)})
	headers.Add(gatewayHeader{Key: "x-ech0-mirror-source-offset", Value: strconv.FormatUint(record.Offset, 10)})
	return headers.Values()
}

func validateMirrorSinkConfig(sink MirrorSinkConfig) error {
	if !validRequiredString(sink.Topic) || !validRequiredString(mirrorSinkAdminURL(sink)) {
		return brokerStoreError(store.CodeInvalidArgument, "mirror sink requires topic and target_admin_url")
	}
	parsed, err := url.ParseRequestURI(mirrorSinkAdminURL(sink))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return brokerStoreError(store.CodeInvalidArgument, "mirror sink %q has invalid target_admin_url", mirrorSinkName(sink))
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return brokerStoreError(store.CodeInvalidArgument, "mirror sink %q requires http or https target_admin_url", mirrorSinkName(sink))
	}
	if !validNonNegativeInt(sink.MaxRecords) {
		return brokerStoreError(store.CodeInvalidArgument, "mirror sink %q max_records cannot be negative", mirrorSinkName(sink))
	}
	return nil
}

func mirrorSinkIdentity(cfg Config, sink MirrorSinkConfig) Identity {
	return Identity{
		Tenant:    valueOr(sink.Tenant, cfg.Governance.DefaultTenant),
		Namespace: valueOr(sink.Namespace, cfg.Governance.DefaultNamespace),
		Principal: valueOr(sink.Principal, mirrorSinkPrincipal),
	}
}

func mirrorSinkName(sink MirrorSinkConfig) string {
	if strings.TrimSpace(sink.Name) != "" {
		return strings.TrimSpace(sink.Name)
	}
	return sink.Topic + "." + strconv.FormatUint(uint64(sink.Partition), 10)
}

func mirrorSinkConsumer(sink MirrorSinkConfig) string {
	if strings.TrimSpace(sink.Consumer) != "" {
		return strings.TrimSpace(sink.Consumer)
	}
	return mirrorSinkDefaultConsumer + "." + mirrorSinkName(sink)
}

func mirrorSinkTargetTopic(sink MirrorSinkConfig) string {
	return valueOr(strings.TrimSpace(sink.TargetTopic), strings.TrimSpace(sink.Topic))
}

func mirrorSinkAdminURL(sink MirrorSinkConfig) string {
	return strings.TrimRight(strings.TrimSpace(sink.TargetAdminURL), "/")
}

func mirrorSinkProduceURL(sink MirrorSinkConfig) string {
	return mirrorSinkAdminURL(sink) + "/api/gateway/topics/" + url.PathEscape(mirrorSinkTargetTopic(sink)) + "/records"
}

func mirrorSinkMaxRecords(sink MirrorSinkConfig) int {
	if sink.MaxRecords == 0 {
		return mirrorSinkDefaultRecords
	}
	return sink.MaxRecords
}

func mirrorSinkTimeout(sink MirrorSinkConfig) time.Duration {
	if sink.TimeoutMS == 0 {
		return mirrorSinkDefaultTimeout
	}
	return durationFromMillis(sink.TimeoutMS)
}
