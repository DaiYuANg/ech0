package broker

import (
	"context"
	"errors"
	"sync"

	"github.com/panjf2000/ants/v2"
)

func runBounded(ctx context.Context, concurrency int64, items int, run func(context.Context, int) error) error {
	if items == 0 {
		return nil
	}
	pool, err := newBoundedWorkerPool(concurrency, items)
	if err != nil {
		return err
	}
	defer pool.Release()

	groupCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make(chan error, items)
	var wg sync.WaitGroup
	for index := range items {
		if err := submitBoundedWorker(groupCtx, pool, &wg, errs, cancel, index, run); err != nil {
			cancel()
			return errors.Join(
				err,
				wrapBroker("bounded_run_failed", waitBoundedWorkers(&wg, errs), "run bounded workers"),
			)
		}
	}
	return wrapBroker("bounded_run_failed", waitBoundedWorkers(&wg, errs), "run bounded workers")
}

func newBoundedWorkerPool(concurrency int64, items int) (*ants.Pool, error) {
	if concurrency <= 0 {
		concurrency = 1
	}
	workerCount := min(int(concurrency), items)
	pool, err := ants.NewPool(workerCount, ants.WithMaxBlockingTasks(items))
	if err != nil {
		return nil, wrapBroker("bounded_worker_pool_create_failed", err, "create bounded worker pool")
	}
	return pool, nil
}

func submitBoundedWorker(
	ctx context.Context,
	pool *ants.Pool,
	wg *sync.WaitGroup,
	errs chan<- error,
	cancel context.CancelFunc,
	index int,
	run func(context.Context, int) error,
) error {
	if err := ctx.Err(); err != nil {
		return wrapBroker("bounded_run_context_done", err, "run bounded workers")
	}
	wg.Add(1)
	if err := pool.Submit(func() {
		defer wg.Done()
		if err := run(ctx, index); err != nil {
			errs <- err
			cancel()
		}
	}); err != nil {
		wg.Done()
		return wrapBroker("bounded_worker_submit_failed", err, "submit bounded worker")
	}
	return nil
}

func waitBoundedWorkers(wg *sync.WaitGroup, errs chan error) error {
	wg.Wait()
	close(errs)
	var result error
	for err := range errs {
		result = errors.Join(result, err)
	}
	return result
}
