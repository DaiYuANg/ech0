package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) SaveProducerBatch(batch ProducerPublishedBatch) error {
	if err := validateProducerBatch(batch); err != nil {
		return err
	}
	err := s.producerBatches.Put(context.Background(), producerBatchKey(batch), cloneProducerPublishedBatch(batch))
	return wrapExternal(err, "save producer batch")
}

func (s *StorxMetadataStore) ListProducerBatches(filter ProducerBatchFilter) ([]ProducerPublishedBatch, error) {
	out := collectionlist.NewList[ProducerPublishedBatch]()
	err := s.producerBatches.Walk(context.Background(), func(entry bboltx.Entry[string, ProducerPublishedBatch]) error {
		if producerBatchMatchesFilter(entry.Value, filter) {
			out.Add(cloneProducerPublishedBatch(entry.Value))
		}
		return nil
	})
	if err != nil {
		return nil, wrapExternal(err, "list producer batches")
	}
	return sortProducerPublishedBatches(out.Values()), nil
}

func (s *StorxMetadataStore) DeleteProducerBatch(batch ProducerPublishedBatch) error {
	return wrapExternal(s.producerBatches.Delete(context.Background(), producerBatchKey(batch)), "delete producer batch")
}
