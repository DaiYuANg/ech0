package ech0

import (
	"context"

	"github.com/samber/oops"
)

type RequestReplyCleanupResult struct {
	RemovedCursors int
}

func (b *Broker) CleanupRequestRepliesOnce(ctx context.Context) (RequestReplyCleanupResult, error) {
	result, err := b.broker.CleanupRequestRepliesOnce(ctx)
	if err != nil {
		return RequestReplyCleanupResult{}, oops.In("embedded").Code("request_reply_cleanup_failed").Wrapf(err, "cleanup request reply cursors")
	}
	return RequestReplyCleanupResult{RemovedCursors: result.RemovedCursors}, nil
}
