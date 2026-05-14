package store

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func restoreMemoryProducerBatches(snapshotBatches collectionlist.List[ProducerPublishedBatch]) *collectionmapping.Map[string, ProducerPublishedBatch] {
	batches := collectionmapping.NewMapWithCapacity[string, ProducerPublishedBatch](snapshotBatches.Len())
	snapshotBatches.Range(func(_ int, batch ProducerPublishedBatch) bool {
		if validateProducerBatch(batch) == nil {
			batches.Set(producerBatchKey(batch), cloneProducerPublishedBatch(batch))
		}
		return true
	})
	return batches
}
