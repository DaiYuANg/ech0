package broker

import (
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/oops"
)

func (b *Broker) snapshot() (store.Snapshot, error) {
	metaSnapshotter, ok := b.meta.(store.Snapshotter)
	if !ok {
		return store.Snapshot{}, oops.In("broker").Code("metadata_snapshot_unsupported").Wrapf(store.E(store.CodeInvalidArgument, "metadata store does not support snapshots"), "create snapshot")
	}
	if b.queue == nil {
		return store.Snapshot{}, oops.In("broker").Code("log_snapshot_unsupported").Wrapf(store.E(store.CodeInvalidArgument, "log store does not support snapshots"), "create snapshot")
	}
	metaSnapshot, err := metaSnapshotter.Snapshot()
	if err != nil {
		return store.Snapshot{}, oops.In("broker").Code("metadata_snapshot_failed").Wrapf(err, "snapshot metadata store")
	}
	logSnapshot, err := b.queue.Snapshot()
	if err != nil {
		return store.Snapshot{}, oops.In("broker").Code("log_snapshot_failed").Wrapf(err, "snapshot log store")
	}
	if metaSnapshot.Topics.IsEmpty() {
		metaSnapshot.Topics = logSnapshot.Topics
	}
	metaSnapshot.Records = logSnapshot.Records
	metaSnapshot.LogOffsets = logSnapshot.LogOffsets
	return metaSnapshot, nil
}

func (b *Broker) restore(snapshot store.Snapshot) error {
	if b.queue == nil {
		return oops.In("broker").Code("log_restore_unsupported").Wrapf(store.E(store.CodeInvalidArgument, "log store does not support snapshots"), "restore snapshot")
	}
	if err := b.queue.Restore(snapshot); err != nil {
		return oops.In("broker").Code("log_restore_failed").Wrapf(err, "restore log store")
	}
	metaSnapshotter, ok := b.meta.(store.Snapshotter)
	if !ok {
		return oops.In("broker").Code("metadata_restore_unsupported").Wrapf(store.E(store.CodeInvalidArgument, "metadata store does not support snapshots"), "restore snapshot")
	}
	return oops.In("broker").Code("metadata_restore_failed").Wrapf(metaSnapshotter.Restore(snapshot), "restore metadata store")
}
