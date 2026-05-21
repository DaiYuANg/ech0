package broker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/go-co-op/gocron/v2"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	databaseOutboxDefaultInterval = 5 * time.Second
	databaseOutboxDefaultRecords  = 100
	databaseOutboxPrincipal       = "database-outbox"
)

type DatabaseOutboxRow struct {
	ID         string
	Topic      string
	Partition  *uint32
	Key        []byte
	RoutingKey string
	Payload    []byte
	Headers    []store.RecordHeader
}

type DatabaseOutboxResult struct {
	Published int
	Marked    int
}

func registerDatabaseOutboxJobs(scheduler gocron.Scheduler, cfg Config, broker *Broker, logger *slog.Logger) error {
	for index := range cfg.Broker.DatabaseOutboxes {
		outbox := cfg.Broker.DatabaseOutboxes[index]
		if err := registerDatabaseOutboxJob(scheduler, broker, logger, outbox); err != nil {
			return err
		}
	}
	return nil
}

func registerDatabaseOutboxJob(
	scheduler gocron.Scheduler,
	broker *Broker,
	logger *slog.Logger,
	outbox DatabaseOutboxConfig,
) error {
	if err := validateDatabaseOutboxConfig(outbox, true); err != nil {
		return err
	}
	name := databaseOutboxName(outbox)
	_, err := scheduler.NewJob(
		gocron.DurationJob(durationFromSeconds(outbox.IntervalSecs, databaseOutboxDefaultInterval)),
		gocron.NewTask(func(ctx context.Context) error {
			result, err := broker.ProcessDatabaseOutboxOnce(ctx, outbox)
			if err != nil {
				return err
			}
			if result.Published > 0 && logger != nil {
				logger.Info("database outbox published records", "name", name, "published", result.Published, "marked", result.Marked)
			}
			return nil
		}),
		gocron.WithName("ech0.database_outbox."+name),
		gocron.WithTags("ech0", "database", "outbox"),
	)
	return wrapBroker("database_outbox_job_register_failed", err, "register database outbox job")
}

func (b *Broker) ProcessDatabaseOutboxOnce(ctx context.Context, outbox DatabaseOutboxConfig) (DatabaseOutboxResult, error) {
	if err := validateDatabaseOutboxConfig(outbox, true); err != nil {
		return DatabaseOutboxResult{}, err
	}
	client, err := openSQLDatabaseOutbox(outbox)
	if err != nil {
		return DatabaseOutboxResult{}, err
	}
	rows, fetchErr := client.Fetch(ctx, databaseOutboxMaxRecords(outbox))
	if fetchErr != nil {
		closeErr := client.Close()
		return DatabaseOutboxResult{}, errors.Join(fetchErr, wrapBroker("database_outbox_close_failed", closeErr, "close database outbox"))
	}
	result, processErr := b.ProcessDatabaseOutboxRows(ctx, outbox, rows, client.MarkDelivered)
	closeErr := client.Close()
	if processErr != nil {
		return result, processErr
	}
	return result, wrapBroker("database_outbox_close_failed", closeErr, "close database outbox")
}

func (b *Broker) ProcessDatabaseOutboxRows(
	ctx context.Context,
	outbox DatabaseOutboxConfig,
	rows []DatabaseOutboxRow,
	markDelivered func(context.Context, string) error,
) (DatabaseOutboxResult, error) {
	if err := validateDatabaseOutboxConfig(outbox, false); err != nil {
		return DatabaseOutboxResult{}, err
	}
	runCtx := WithIdentity(ctx, databaseOutboxIdentity(b.cfg, outbox))
	result := DatabaseOutboxResult{}
	for index := range rows {
		row := rows[index]
		if err := b.publishDatabaseOutboxRow(runCtx, outbox, row); err != nil {
			return result, err
		}
		result.Published++
		if markDelivered != nil && strings.TrimSpace(row.ID) != "" {
			if err := markDelivered(ctx, row.ID); err != nil {
				return result, wrapBroker("database_outbox_mark_failed", err, "mark outbox row delivered")
			}
			result.Marked++
		}
	}
	return result, nil
}

func (b *Broker) publishDatabaseOutboxRow(ctx context.Context, outbox DatabaseOutboxConfig, row DatabaseOutboxRow) error {
	topic := valueOr(strings.TrimSpace(row.Topic), strings.TrimSpace(outbox.Topic))
	if topic == "" {
		return brokerStoreError(store.CodeInvalidArgument, "database outbox row requires topic")
	}
	record := store.NewRecordAppend(row.Payload)
	record.Key = append([]byte(nil), row.Key...)
	record.Headers = databaseOutboxHeaders(outbox, row)
	_, err := b.PublishRecord(ctx, topic, databaseOutboxPartitioning(row), record)
	return err
}

func databaseOutboxHeaders(outbox DatabaseOutboxConfig, row DatabaseOutboxRow) []store.RecordHeader {
	headers := collectionlist.NewListWithCapacity[store.RecordHeader](len(outbox.Headers) + len(row.Headers))
	for index := range outbox.Headers {
		header := outbox.Headers[index]
		if strings.TrimSpace(header.Key) != "" {
			headers.Add(store.RecordHeader{Key: header.Key, Value: []byte(header.Value)})
		}
	}
	headers.Add(row.Headers...)
	return headers.Values()
}

func databaseOutboxPartitioning(row DatabaseOutboxRow) PublishPartitioning {
	if row.Partition != nil {
		return PublishPartitioning{Mode: PartitionExplicit, Partition: *row.Partition}
	}
	if strings.TrimSpace(row.RoutingKey) != "" {
		return PublishPartitioning{Mode: PartitionRoutingKeyHash, RoutingKey: row.RoutingKey}
	}
	if len(row.Key) > 0 {
		return PublishPartitioning{Mode: PartitionKeyHash}
	}
	return PublishPartitioning{Mode: PartitionRoundRobin}
}

func validateDatabaseOutboxConfig(outbox DatabaseOutboxConfig, requireSQL bool) error {
	if requireSQL && (strings.TrimSpace(outbox.DriverName) == "" || strings.TrimSpace(outbox.Query) == "") {
		return brokerStoreError(store.CodeInvalidArgument, "database outbox requires driver_name and query")
	}
	if strings.TrimSpace(outbox.Topic) == "" && strings.TrimSpace(outbox.Query) == "" {
		return brokerStoreError(store.CodeInvalidArgument, "database outbox requires topic or query-provided topic")
	}
	if outbox.MaxRecords < 0 {
		return brokerStoreError(store.CodeInvalidArgument, "database outbox %q max_records cannot be negative", databaseOutboxName(outbox))
	}
	return nil
}

func databaseOutboxIdentity(cfg Config, outbox DatabaseOutboxConfig) Identity {
	return Identity{
		Tenant:    valueOr(outbox.Tenant, cfg.Governance.DefaultTenant),
		Namespace: valueOr(outbox.Namespace, cfg.Governance.DefaultNamespace),
		Principal: valueOr(outbox.Principal, databaseOutboxPrincipal),
	}
}

func databaseOutboxName(outbox DatabaseOutboxConfig) string {
	if strings.TrimSpace(outbox.Name) != "" {
		return strings.TrimSpace(outbox.Name)
	}
	return valueOr(strings.TrimSpace(outbox.Topic), "outbox")
}

func databaseOutboxMaxRecords(outbox DatabaseOutboxConfig) int {
	if outbox.MaxRecords == 0 {
		return databaseOutboxDefaultRecords
	}
	return outbox.MaxRecords
}
