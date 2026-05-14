package store

import (
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) Snapshot() (Snapshot, error) {
	catalog, err := s.snapshotCatalogMetadata()
	if err != nil {
		return Snapshot{}, err
	}
	runtime, err := s.snapshotRuntimeMetadata()
	if err != nil {
		return Snapshot{}, err
	}
	return buildStorxSnapshot(catalog, runtime), nil
}

type storxSnapshotCatalog struct {
	topics         []TopicConfig
	offsets        *collectionmapping.Map[string, uint64]
	offsetStates   []ConsumerOffsetState
	consumerPauses []ConsumerPauseState
	placements     []ShardPlacement
	members        []ConsumerGroupMember
	assignments    []ConsumerGroupAssignment
}

type storxSnapshotRuntime struct {
	state             *BrokerState
	transactions      []TransactionState
	producerBatches   []ProducerPublishedBatch
	aclPolicies       []ACLPolicy
	nextTransactionID uint64
}

func (s *StorxMetadataStore) snapshotCatalogMetadata() (storxSnapshotCatalog, error) {
	topics, err := s.ListTopics()
	if err != nil {
		return storxSnapshotCatalog{}, err
	}
	members, err := s.listAllMembers()
	if err != nil {
		return storxSnapshotCatalog{}, err
	}
	assignments, err := s.ListGroupAssignments()
	if err != nil {
		return storxSnapshotCatalog{}, err
	}
	offsets, err := s.snapshotOffsets()
	if err != nil {
		return storxSnapshotCatalog{}, err
	}
	offsetStates, err := s.ListConsumerOffsetStates()
	if err != nil {
		return storxSnapshotCatalog{}, err
	}
	consumerPauses, err := s.ListConsumerPauses()
	if err != nil {
		return storxSnapshotCatalog{}, err
	}
	placements, err := s.ListShardPlacements()
	if err != nil {
		return storxSnapshotCatalog{}, err
	}
	return storxSnapshotCatalog{
		topics:         topics,
		offsets:        offsets,
		offsetStates:   offsetStates,
		consumerPauses: consumerPauses,
		placements:     placements,
		members:        members,
		assignments:    assignments,
	}, nil
}

func (s *StorxMetadataStore) snapshotRuntimeMetadata() (storxSnapshotRuntime, error) {
	state, err := s.LoadBrokerState()
	if err != nil {
		return storxSnapshotRuntime{}, err
	}
	transactions, err := s.ListTransactions()
	if err != nil {
		return storxSnapshotRuntime{}, err
	}
	producerBatches, err := s.ListProducerBatches(ProducerBatchFilter{})
	if err != nil {
		return storxSnapshotRuntime{}, err
	}
	aclPolicies, err := s.ListACLPolicies(ACLPolicyFilter{})
	if err != nil {
		return storxSnapshotRuntime{}, err
	}
	nextTransactionID, err := s.loadNextTransactionID()
	if err != nil {
		return storxSnapshotRuntime{}, err
	}
	return storxSnapshotRuntime{
		state:             state,
		transactions:      transactions,
		producerBatches:   producerBatches,
		aclPolicies:       aclPolicies,
		nextTransactionID: nextTransactionID,
	}, nil
}

func (s *StorxMetadataStore) snapshotOffsets() (*collectionmapping.Map[string, uint64], error) {
	offsets := collectionmapping.NewMap[string, uint64]()
	err := s.offsets.Walk(context.Background(), func(entry bboltx.Entry[string, uint64]) error {
		offsets.Set(entry.Key, entry.Value)
		return nil
	})
	if err != nil {
		return nil, wrapExternal(err, "walk consumer offsets")
	}
	return offsets, nil
}

func buildStorxSnapshot(catalog storxSnapshotCatalog, runtime storxSnapshotRuntime) Snapshot {
	return Snapshot{
		Topics:            *collectionlist.NewListWithCapacity[TopicConfig](len(catalog.topics), catalog.topics...),
		Offsets:           *catalog.offsets,
		OffsetStates:      *collectionlist.NewListWithCapacity[ConsumerOffsetState](len(catalog.offsetStates), catalog.offsetStates...),
		ConsumerPauses:    *collectionlist.NewListWithCapacity[ConsumerPauseState](len(catalog.consumerPauses), catalog.consumerPauses...),
		Placements:        *collectionlist.NewListWithCapacity[ShardPlacement](len(catalog.placements), catalog.placements...),
		Members:           *collectionlist.NewListWithCapacity[ConsumerGroupMember](len(catalog.members), catalog.members...),
		Assignments:       *collectionlist.NewListWithCapacity[ConsumerGroupAssignment](len(catalog.assignments), catalog.assignments...),
		Transactions:      *collectionlist.NewListWithCapacity[TransactionState](len(runtime.transactions), runtime.transactions...),
		ProducerBatches:   *collectionlist.NewListWithCapacity[ProducerPublishedBatch](len(runtime.producerBatches), runtime.producerBatches...),
		ACLPolicies:       *collectionlist.NewListWithCapacity[ACLPolicy](len(runtime.aclPolicies), runtime.aclPolicies...),
		NextTransactionID: runtime.nextTransactionID,
		BrokerState:       runtime.state,
	}
}
