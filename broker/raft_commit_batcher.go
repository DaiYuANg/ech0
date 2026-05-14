package broker

import (
	"context"
	"sync"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

type raftCommitBatcher struct {
	mu      sync.Mutex
	running bool
	pending []*commitOffsetWork
}

type commitOffsetWork struct {
	ctx context.Context
	req commitOffsetCommand
	ch  chan error
}

func newRaftCommitBatcher() *raftCommitBatcher {
	return &raftCommitBatcher{}
}

func (b *Broker) proposeCommitOffsetCoalesced(ctx context.Context, req commitOffsetCommand) error {
	work := &commitOffsetWork{
		ctx: ctx,
		req: req,
		ch:  make(chan error, 1),
	}
	if b.commitBatcher.enqueue(work) {
		go b.runCommitBatcher(context.WithoutCancel(ctx))
	}
	select {
	case err := <-work.ch:
		return err
	case <-ctx.Done():
		return wrapBroker("raft_commit_offset_context_done", ctx.Err(), "commit offset")
	}
}

func (q *raftCommitBatcher) enqueue(work *commitOffsetWork) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, work)
	if q.running {
		return false
	}
	q.running = true
	return true
}

func (b *Broker) runCommitBatcher(ctx context.Context) {
	for {
		waitRaftCoalesceWindow(ctx)
		works := b.commitBatcher.drain()
		if len(works) > 0 {
			b.applyCommitOffsetWorks(ctx, works)
		}
		if b.commitBatcher.finishIfIdle() {
			return
		}
	}
}

func (q *raftCommitBatcher) drain() []*commitOffsetWork {
	q.mu.Lock()
	defer q.mu.Unlock()
	works := q.pending
	q.pending = nil
	return works
}

func (q *raftCommitBatcher) finishIfIdle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) > 0 {
		return false
	}
	q.running = false
	return true
}

func (b *Broker) applyCommitOffsetWorks(ctx context.Context, works []*commitOffsetWork) {
	active := collectionlist.NewListWithCapacity[*commitOffsetWork](len(works))
	requests := collectionlist.NewListWithCapacity[commitOffsetCommand](len(works))
	for _, work := range works {
		if err := work.ctx.Err(); err != nil {
			work.ch <- wrapBroker("raft_commit_offset_context_done", err, "commit offset")
			continue
		}
		active.Add(work)
		requests.Add(work.req)
	}
	if active.Len() == 0 {
		return
	}
	value, err := b.applyCoalescedRaftCommand(firstCommitOffsetContext(ctx, active), raftCommandCommitOffsets, commitOffsetsCommand{Requests: requests.Values()})
	if err != nil {
		deliverCommitOffsetError(active.Values(), err)
		return
	}
	result, err := raftValueAs[commitOffsetsResult](value)
	if err != nil {
		deliverCommitOffsetError(active.Values(), err)
		return
	}
	deliverCommitOffsetResults(active.Values(), result)
}

func firstCommitOffsetContext(ctx context.Context, works *collectionlist.List[*commitOffsetWork]) context.Context {
	values := works.Values()
	if len(values) == 0 {
		return ctx
	}
	return values[0].ctx
}

func deliverCommitOffsetError(works []*commitOffsetWork, err error) {
	for _, work := range works {
		work.ch <- err
	}
}

func deliverCommitOffsetResults(works []*commitOffsetWork, result commitOffsetsResult) {
	if len(result.Items) != len(works) {
		deliverCommitOffsetError(works, brokerStoreError(store.CodeCodec, "commit offset result length mismatch: got %d want %d", len(result.Items), len(works)))
		return
	}
	for index, work := range works {
		work.ch <- errorFromMessage(result.Items[index].Error)
	}
}
