package broker

import (
	"context"
	"errors"

	dragonboat "github.com/lni/dragonboat/v4"
)

func (n *raftNode) Snapshot(ctx context.Context) error {
	if err := n.validateApply(ctx); err != nil {
		return err
	}
	var result error
	for _, groupID := range n.groups {
		snapshotCtx, cancel := contextWithRaftApplyTimeout(ctx, n.cfg.Raft.ApplyTimeoutMS)
		_, err := n.nodeHost.SyncRequestSnapshot(snapshotCtx, groupID, dragonboat.DefaultSnapshotOption)
		cancel()
		if errors.Is(err, dragonboat.ErrRejected) {
			continue
		}
		if err != nil {
			result = errors.Join(result, wrapBroker("raft_snapshot_failed", err, "snapshot raft group %d", groupID))
		}
	}
	return result
}
