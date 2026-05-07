package store

import (
	"context"

	"github.com/arcgolabs/storx/badgerx"
)

func scanBadgerNamespace[K any, V any](
	ctx context.Context,
	namespace *badgerx.Namespace[K, V],
) ([]badgerx.Entry[K, V], error) {
	entries := make([]badgerx.Entry[K, V], 0)
	err := namespace.View(ctx, func(tx badgerx.ViewTx[K, V]) error {
		return tx.Scan(func(key K, value V) error {
			entries = append(entries, badgerx.Entry[K, V]{
				Key:   key,
				Value: value,
			})
			return nil
		})
	})
	return entries, wrapExternal(err, "scan badger namespace")
}

func scanBadgerNamespaceKeys[K any, V any](
	ctx context.Context,
	namespace *badgerx.Namespace[K, V],
) ([]K, error) {
	entries, err := scanBadgerNamespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	keys := make([]K, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, entry.Key)
	}
	return keys, nil
}
