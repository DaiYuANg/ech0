package broker

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	fileSinkDefaultInterval = 5 * time.Second
	fileSinkDefaultRecords  = 100
	fileSinkDefaultConsumer = "__file_sink"
	fileSinkPrincipal       = "file-sink"
)

type FileSinkResult struct {
	Delivered           int
	CommittedNextOffset *uint64
}

type fileSinkEnvelope struct {
	Sink      string                `json:"sink"`
	Consumer  string                `json:"consumer"`
	Topic     string                `json:"topic"`
	Partition uint32                `json:"partition"`
	Record    gatewayRecordResponse `json:"record"`
}

func registerFileSinkJobs(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	for index := range cfg.Broker.FileSinks {
		sink := cfg.Broker.FileSinks[index]
		if err := registerFileSinkJob(scheduler, broker, logger, sink); err != nil {
			return err
		}
	}
	return nil
}

func registerFileSinkJob(
	scheduler gocron.Scheduler,
	broker *Broker,
	logger *slog.Logger,
	sink FileSinkConfig,
) error {
	if err := validateFileSinkConfig(sink); err != nil {
		return err
	}
	name := fileSinkName(sink)
	_, err := scheduler.NewJob(
		gocron.DurationJob(durationFromSeconds(sink.IntervalSecs, fileSinkDefaultInterval)),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.ProcessFileSinkOnce(ctx, sink)
			if err != nil {
				return err
			}
			if result.Delivered > 0 && logger != nil {
				logger.Info("file sink wrote records", "name", name, "topic", sink.Topic, "partition", sink.Partition, "delivered", result.Delivered)
			}
			return nil
		}),
		gocron.WithName("ech0.file_sink."+name),
		gocron.WithTags("ech0", "file", "sink"),
	)
	return wrapBroker("file_sink_job_register_failed", err, "register file sink job")
}

func (b *Broker) ProcessFileSinkOnce(ctx context.Context, sink FileSinkConfig) (FileSinkResult, error) {
	if validateErr := validateFileSinkConfig(sink); validateErr != nil {
		return FileSinkResult{}, validateErr
	}
	identity := fileSinkIdentity(b.cfg, sink)
	runCtx := WithIdentity(ctx, identity)
	consumer := fileSinkConsumer(sink)
	poll, err := b.FetchWithIsolation(runCtx, consumer, sink.Topic, sink.Partition, nil, fileSinkMaxRecords(sink), FetchIsolationReadCommitted)
	if err != nil {
		return FileSinkResult{}, err
	}
	if len(poll.Records) == 0 {
		return FileSinkResult{}, nil
	}
	file, err := openFileSinkAppend(sink)
	if err != nil {
		return FileSinkResult{}, err
	}
	result, writeErr := b.writeFileSinkRecords(runCtx, file, sink, consumer, poll.Records)
	closeErr := file.Close()
	if writeErr != nil {
		return result, writeErr
	}
	return result, wrapBroker("file_sink_close_failed", closeErr, "close file sink")
}

func (b *Broker) writeFileSinkRecords(
	ctx context.Context,
	file *os.File,
	sink FileSinkConfig,
	consumer string,
	records []store.Record,
) (FileSinkResult, error) {
	encoder := newJSONEncoder(file)
	result := FileSinkResult{}
	metadata := "file:" + fileSinkName(sink)
	for index := range records {
		record := records[index]
		if err := encoder.Encode(fileSinkRecordEnvelope(sink, consumer, record)); err != nil {
			return result, wrapBroker("file_sink_encode_failed", err, "write file sink record")
		}
		if err := file.Sync(); err != nil {
			return result, wrapBroker("file_sink_sync_failed", err, "sync file sink")
		}
		nextOffset := record.Offset + 1
		if err := b.CommitOffsetWithMetadata(ctx, consumer, sink.Topic, sink.Partition, nextOffset, metadata); err != nil {
			return result, err
		}
		result.Delivered++
		result.CommittedNextOffset = &nextOffset
	}
	return result, nil
}

func openFileSinkAppend(sink FileSinkConfig) (*os.File, error) {
	path := fileSinkPath(sink)
	dir := filepath.Dir(path)
	name := filepath.Base(path)
	if dir != "." {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, wrapBroker("file_sink_directory_create_failed", err, "create file sink directory")
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, wrapBroker("file_sink_root_open_failed", err, "open file sink root")
	}
	file, err := root.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	closeRootErr := root.Close()
	if err != nil {
		return nil, errors.Join(
			wrapBroker("file_sink_open_failed", err, "open file sink"),
			wrapBroker("file_sink_root_close_failed", closeRootErr, "close file sink root"),
		)
	}
	if closeRootErr != nil {
		return nil, errors.Join(
			wrapBroker("file_sink_root_close_failed", closeRootErr, "close file sink root"),
			wrapBroker("file_sink_close_failed", file.Close(), "close file sink"),
		)
	}
	return file, nil
}

func fileSinkRecordEnvelope(sink FileSinkConfig, consumer string, record store.Record) fileSinkEnvelope {
	return fileSinkEnvelope{
		Sink:      fileSinkName(sink),
		Consumer:  consumer,
		Topic:     sink.Topic,
		Partition: sink.Partition,
		Record:    gatewayRecord(record),
	}
}

func validateFileSinkConfig(sink FileSinkConfig) error {
	if !validRequiredString(sink.Topic) || !validRequiredString(fileSinkPath(sink)) {
		return brokerStoreError(store.CodeInvalidArgument, "file sink requires topic and path")
	}
	if !validNonNegativeInt(sink.MaxRecords) {
		return brokerStoreError(store.CodeInvalidArgument, "file sink %q max_records cannot be negative", fileSinkName(sink))
	}
	return nil
}

func fileSinkIdentity(cfg Config, sink FileSinkConfig) Identity {
	return Identity{
		Tenant:    valueOr(sink.Tenant, cfg.Governance.DefaultTenant),
		Namespace: valueOr(sink.Namespace, cfg.Governance.DefaultNamespace),
		Principal: valueOr(sink.Principal, fileSinkPrincipal),
	}
}

func fileSinkName(sink FileSinkConfig) string {
	if strings.TrimSpace(sink.Name) != "" {
		return strings.TrimSpace(sink.Name)
	}
	return sink.Topic + "." + strconv.FormatUint(uint64(sink.Partition), 10)
}

func fileSinkConsumer(sink FileSinkConfig) string {
	if strings.TrimSpace(sink.Consumer) != "" {
		return strings.TrimSpace(sink.Consumer)
	}
	return fileSinkDefaultConsumer + "." + fileSinkName(sink)
}

func fileSinkPath(sink FileSinkConfig) string {
	return strings.TrimSpace(sink.Path)
}

func fileSinkMaxRecords(sink FileSinkConfig) int {
	if sink.MaxRecords == 0 {
		return fileSinkDefaultRecords
	}
	return sink.MaxRecords
}
