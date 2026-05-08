package broker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
)

const raftCoalesceWindow = time.Millisecond

type raftProduceBatcher struct {
	mu      sync.Mutex
	running bool
	pending []*produceBatchWork
}

type produceBatchWork struct {
	ctx context.Context
	req produceBatchCommand
	ch  chan produceBatchWorkResult
}

type produceBatchWorkResult struct {
	result ProduceBatchResult
	err    error
}

func newRaftProduceBatcher() *raftProduceBatcher {
	return &raftProduceBatcher{}
}

func (b *Broker) proposeProduceBatchCoalesced(ctx context.Context, req produceBatchCommand) (ProduceBatchResult, error) {
	work := &produceBatchWork{
		ctx: ctx,
		req: req,
		ch:  make(chan produceBatchWorkResult, 1),
	}
	if b.produceBatcher.enqueue(work) {
		go b.runProduceBatcher(context.WithoutCancel(ctx))
	}
	select {
	case result := <-work.ch:
		return result.result, result.err
	case <-ctx.Done():
		return ProduceBatchResult{}, wrapBroker("raft_produce_batch_context_done", ctx.Err(), "produce batch")
	}
}

func (q *raftProduceBatcher) enqueue(work *produceBatchWork) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, work)
	if q.running {
		return false
	}
	q.running = true
	return true
}

func (b *Broker) runProduceBatcher(ctx context.Context) {
	for {
		waitRaftCoalesceWindow(ctx)
		works := b.produceBatcher.drain()
		if len(works) > 0 {
			b.applyProduceBatchWorks(ctx, works)
		}
		if b.produceBatcher.finishIfIdle() {
			return
		}
	}
}

func (q *raftProduceBatcher) drain() []*produceBatchWork {
	q.mu.Lock()
	defer q.mu.Unlock()
	works := q.pending
	q.pending = nil
	return works
}

func (q *raftProduceBatcher) finishIfIdle() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) > 0 {
		return false
	}
	q.running = false
	return true
}

func (b *Broker) applyProduceBatchWorks(ctx context.Context, works []*produceBatchWork) {
	active := collectionlist.NewListWithCapacity[*produceBatchWork](len(works))
	requests := collectionlist.NewListWithCapacity[produceBatchCommand](len(works))
	for _, work := range works {
		if err := work.ctx.Err(); err != nil {
			work.ch <- produceBatchWorkResult{err: wrapBroker("raft_produce_batch_context_done", err, "produce batch")}
			continue
		}
		active.Add(work)
		requests.Add(work.req)
	}
	if active.Len() == 0 {
		return
	}
	value, err := b.applyCoalescedRaftCommand(firstProduceBatchContext(ctx, active), raftCommandProduceBatches, produceBatchesCommand{Requests: requests.Values()})
	if err != nil {
		deliverProduceBatchError(active.Values(), err)
		return
	}
	result, err := raftValueAs[produceBatchesResult](value)
	if err != nil {
		deliverProduceBatchError(active.Values(), err)
		return
	}
	deliverProduceBatchResults(active.Values(), result)
}

func firstProduceBatchContext(ctx context.Context, works *collectionlist.List[*produceBatchWork]) context.Context {
	values := works.Values()
	if len(values) == 0 {
		return ctx
	}
	return values[0].ctx
}

func deliverProduceBatchError(works []*produceBatchWork, err error) {
	for _, work := range works {
		work.ch <- produceBatchWorkResult{err: err}
	}
}

func deliverProduceBatchResults(works []*produceBatchWork, result produceBatchesResult) {
	if len(result.Items) != len(works) {
		deliverProduceBatchError(works, brokerStoreError(store.CodeCodec, "produce batch result length mismatch: got %d want %d", len(result.Items), len(works)))
		return
	}
	for index, work := range works {
		item := result.Items[index]
		work.ch <- produceBatchWorkResult{result: item.Result, err: errorFromMessage(item.Error)}
	}
}

func (b *Broker) applyCoalescedRaftCommand(ctx context.Context, commandType string, payload any) (any, error) {
	return b.routeCoalescedPartitionCommand(ctx, commandType, payload)
}

func (b *Broker) currentRaftNode() *raftNode {
	if b == nil {
		return nil
	}
	b.raftMu.RLock()
	node := b.raft
	b.raftMu.RUnlock()
	return node
}

func errorFromMessage(message string) error {
	if message == "" {
		return nil
	}
	return errors.New(message)
}

func waitRaftCoalesceWindow(ctx context.Context) {
	timer := time.NewTimer(raftCoalesceWindow)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
