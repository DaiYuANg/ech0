package broker

import "context"

func (b *Broker) routeCommitOffset(ctx context.Context, req commitOffsetCommand) error {
	if b.usesClusterCommandRouter() {
		return b.proposeCommitOffsetCoalesced(ctx, req)
	}
	_, err := routePartitionCommand(ctx, b, exactPartitionCommandTarget(req.Topic, req.Partition), raftCommandCommitOffset, req, b.applyCommitOffset)
	return err
}
