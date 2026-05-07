package broker

import (
	"context"
	"errors"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func runBounded(ctx context.Context, concurrency int64, items int, run func(context.Context, int) error) error {
	if items == 0 {
		return nil
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	group, groupCtx := errgroup.WithContext(ctx)
	limit := semaphore.NewWeighted(concurrency)
	for i := range items {
		if err := limit.Acquire(groupCtx, 1); err != nil {
			return errors.Join(
				wrapBroker("bounded_run_acquire_failed", err, "acquire bounded worker slot"),
				wrapBroker("bounded_run_failed", group.Wait(), "run bounded workers"),
			)
		}
		index := i
		group.Go(func() error {
			defer limit.Release(1)
			return run(groupCtx, index)
		})
	}
	return wrapBroker("bounded_run_failed", group.Wait(), "run bounded workers")
}
