package store

import (
	"context"

	"github.com/arcgolabs/storx/bboltx"
)

type bucketClearer[K any, V any] struct {
	bucket *bboltx.Bucket[K, V]
}

func (c bucketClearer[K, V]) Clear(ctx context.Context) error {
	keys, err := c.bucket.Keys(ctx)
	if err != nil {
		return wrapExternal(err, "list bucket keys")
	}
	if err := c.bucket.DeleteMany(ctx, keys...); err != nil {
		return wrapExternal(err, "delete bucket keys")
	}
	return nil
}
