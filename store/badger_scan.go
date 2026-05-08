package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/badgerx"
)

func scanBadgerNamespace[K any, V any](
	ctx context.Context,
	namespace *badgerx.Namespace[K, V],
) ([]badgerx.Entry[K, V], error) {
	entries := collectionlist.NewList[badgerx.Entry[K, V]]()
	err := namespace.View(ctx, func(tx badgerx.ViewTx[K, V]) error {
		return tx.Scan(func(key K, value V) error {
			entries.Add(badgerx.Entry[K, V]{
				Key:   key,
				Value: value,
			})
			return nil
		})
	})
	return entries.Values(), wrapExternal(err, "scan badger namespace")
}

func scanBadgerNamespaceKeys[K any, V any](
	ctx context.Context,
	namespace *badgerx.Namespace[K, V],
) ([]K, error) {
	entries, err := scanBadgerNamespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	keys := collectionlist.NewListWithCapacity[K](len(entries))
	for _, entry := range entries {
		keys.Add(entry.Key)
	}
	return keys.Values(), nil
}
