package ech0

import (
	"context"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
)

func stopScheduler(ctx context.Context, scheduled *internalbroker.ScheduledRuntime) error {
	if scheduled == nil {
		return nil
	}
	if err := scheduled.Stop(ctx); err != nil {
		return oops.In("embedded").Code("scheduler_stop_failed").Wrapf(err, "stop scheduler")
	}
	return nil
}

func stopInternalBroker(ctx context.Context, broker *internalbroker.Broker) error {
	if broker == nil {
		return nil
	}
	if err := broker.Stop(ctx); err != nil {
		return oops.In("embedded").Code("broker_stop_failed").Wrapf(err, "stop broker")
	}
	return nil
}

func closeMetadataStore(metaStore metadataStore) error {
	if metaStore == nil {
		return nil
	}
	closer, ok := metaStore.(interface{ Close() error })
	if !ok {
		return nil
	}
	if err := closer.Close(); err != nil {
		return oops.In("embedded").Code("metadata_store_close_failed").Wrapf(err, "close metadata store")
	}
	return nil
}

func closeLogStore(logStore *store.StorxLogStore) error {
	if logStore == nil {
		return nil
	}
	if err := logStore.Close(); err != nil {
		return oops.In("embedded").Code("log_store_close_failed").Wrapf(err, "close log store")
	}
	return nil
}
