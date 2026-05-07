package broker

import "github.com/DaiYuANg/ech0/store"

func (b *Broker) snapshot() (store.Snapshot, error) {
	metaSnapshotter, ok := b.meta.(store.Snapshotter)
	if !ok {
		return store.Snapshot{}, store.E(store.CodeInvalidArgument, "metadata store does not support snapshots")
	}
	logSnapshotter, ok := b.log.(store.Snapshotter)
	if !ok {
		return store.Snapshot{}, store.E(store.CodeInvalidArgument, "log store does not support snapshots")
	}
	metaSnapshot, err := metaSnapshotter.Snapshot()
	if err != nil {
		return store.Snapshot{}, err
	}
	logSnapshot, err := logSnapshotter.Snapshot()
	if err != nil {
		return store.Snapshot{}, err
	}
	if len(metaSnapshot.Topics) == 0 {
		metaSnapshot.Topics = logSnapshot.Topics
	}
	metaSnapshot.Records = logSnapshot.Records
	return metaSnapshot, nil
}

func (b *Broker) restore(snapshot store.Snapshot) error {
	logSnapshotter, ok := b.log.(store.Snapshotter)
	if !ok {
		return store.E(store.CodeInvalidArgument, "log store does not support snapshots")
	}
	if err := logSnapshotter.Restore(snapshot); err != nil {
		return err
	}
	metaSnapshotter, ok := b.meta.(store.Snapshotter)
	if !ok {
		return store.E(store.CodeInvalidArgument, "metadata store does not support snapshots")
	}
	return metaSnapshotter.Restore(snapshot)
}
