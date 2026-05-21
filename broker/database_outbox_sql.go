package broker

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"

	"github.com/lyonbrown4d/ech0/store"
)

type sqlDatabaseOutbox struct {
	cfg DatabaseOutboxConfig
	db  *sql.DB
}

func openSQLDatabaseOutbox(cfg DatabaseOutboxConfig) (*sqlDatabaseOutbox, error) {
	db, err := sql.Open(strings.TrimSpace(cfg.DriverName), strings.TrimSpace(cfg.DSN))
	if err != nil {
		return nil, wrapBroker("database_outbox_open_failed", err, "open database outbox")
	}
	return &sqlDatabaseOutbox{cfg: cfg, db: db}, nil
}

func (o *sqlDatabaseOutbox) Fetch(ctx context.Context, maxRecords int) (out []DatabaseOutboxRow, err error) {
	rows, err := o.db.QueryContext(ctx, o.cfg.Query)
	if err != nil {
		return nil, wrapBroker("database_outbox_query_failed", err, "query database outbox")
	}
	defer func() {
		closeErr := rows.Close()
		if err != nil {
			err = errorsJoinDatabaseOutbox(err, closeErr)
			return
		}
		err = wrapBroker("database_outbox_rows_close_failed", closeErr, "close database outbox rows")
	}()
	return readSQLDatabaseOutboxRows(rows, maxRecords)
}

func readSQLDatabaseOutboxRows(rows *sql.Rows, maxRecords int) ([]DatabaseOutboxRow, error) {
	out := make([]DatabaseOutboxRow, 0, maxRecords)
	for rows.Next() && len(out) < maxRecords {
		row, scanErr := scanDatabaseOutboxRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, row)
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, wrapBroker("database_outbox_rows_failed", rowsErr, "read database outbox rows")
	}
	return out, nil
}

func (o *sqlDatabaseOutbox) MarkDelivered(ctx context.Context, id string) error {
	if strings.TrimSpace(o.cfg.MarkDeliveredSQL) == "" {
		return nil
	}
	if strings.TrimSpace(id) == "" {
		return brokerStoreError(store.CodeInvalidArgument, "database outbox row id is required to mark delivered")
	}
	_, err := o.db.ExecContext(ctx, o.cfg.MarkDeliveredSQL, id)
	return wrapBroker("database_outbox_mark_failed", err, "mark database outbox row delivered")
}

func (o *sqlDatabaseOutbox) Close() error {
	if o == nil || o.db == nil {
		return nil
	}
	return wrapBroker("database_outbox_close_failed", o.db.Close(), "close database outbox")
}

func scanDatabaseOutboxRow(scanner interface {
	Scan(dest ...any) error
}) (DatabaseOutboxRow, error) {
	var row databaseOutboxSQLRow
	if err := scanner.Scan(&row.ID, &row.Topic, &row.Partition, &row.Key, &row.RoutingKey, &row.Payload); err != nil {
		return DatabaseOutboxRow{}, wrapBroker("database_outbox_scan_failed", err, "scan database outbox row")
	}
	return row.toOutboxRow(), nil
}

type databaseOutboxSQLRow struct {
	ID         sql.NullString
	Topic      sql.NullString
	Partition  sql.NullInt64
	Key        []byte
	RoutingKey sql.NullString
	Payload    []byte
}

func (r databaseOutboxSQLRow) toOutboxRow() DatabaseOutboxRow {
	out := DatabaseOutboxRow{
		ID:         r.ID.String,
		Topic:      r.Topic.String,
		Key:        append([]byte(nil), r.Key...),
		RoutingKey: r.RoutingKey.String,
		Payload:    append([]byte(nil), r.Payload...),
	}
	if r.Partition.Valid && r.Partition.Int64 >= 0 {
		parsed, err := strconv.ParseUint(strconv.FormatInt(r.Partition.Int64, 10), 10, 32)
		if err != nil {
			return out
		}
		value := uint32(parsed)
		out.Partition = &value
	}
	return out
}

func errorsJoinDatabaseOutbox(current, closeErr error) error {
	if current != nil {
		return errors.Join(current, wrapBroker("database_outbox_rows_close_failed", closeErr, "close database outbox rows"))
	}
	return wrapBroker("database_outbox_rows_close_failed", closeErr, "close database outbox rows")
}
