package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/arcgolabs/storx/bboltx"
	"github.com/arcgolabs/storx/codec"
	"github.com/arcgolabs/storx/keycodec"
)

func OpenStorxMetadataStore(path string) (*StorxMetadataStore, error) {
	return OpenStorxMetadataStoreWithOptionsContext(context.Background(), path, StorxMetadataOptions{})
}

func OpenStorxMetadataStoreWithOptions(path string, options StorxMetadataOptions) (*StorxMetadataStore, error) {
	return OpenStorxMetadataStoreWithOptionsContext(context.Background(), path, options)
}

func OpenStorxMetadataStoreWithOptionsContext(ctx context.Context, path string, _ StorxMetadataOptions) (*StorxMetadataStore, error) {
	if path == "" {
		return nil, E(CodeInvalidArgument, "metadata path is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, wrapExternal(err, "open metadata store")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, wrapExternal(err, "create metadata directory")
	}
	db, err := bboltx.Open(path, 0o600, nil)
	if err != nil {
		return nil, wrapExternal(err, "open metadata store")
	}
	store := newStorxMetadataStore(db)
	if err := store.members.RebuildIndexes(ctx); err != nil {
		return nil, errors.Join(wrapExternal(err, "rebuild group member indexes"), store.Close())
	}
	return store, nil
}

func newStorxMetadataStore(db *bboltx.DB) *StorxMetadataStore {
	keyCodec := keycodec.String()
	membersByGroup := bboltx.NewSecondaryIndexManyWithDB[string, ConsumerGroupMember, string](
		db,
		bucketMembersGroup,
		keyCodec,
		keyCodec,
		func(value ConsumerGroupMember) string { return value.Group },
	)
	members := bboltx.NewModelStoreWithDB(
		db,
		bucketMembers,
		keyCodec,
		codec.JSON[ConsumerGroupMember](),
		func(value ConsumerGroupMember) string {
			return groupMemberKey(value.Group, value.MemberID)
		},
		bboltx.WithModelIndex[string, ConsumerGroupMember](membersByGroup),
	)
	return &StorxMetadataStore{
		db:                  db,
		topics:              bboltx.NewBucketWithDB(db, bucketTopics, keyCodec, codec.JSON[TopicConfig]()),
		offsets:             bboltx.NewBucketWithDB(db, bucketOffsets, keyCodec, codec.JSON[uint64]()),
		members:             members,
		membersByGroup:      membersByGroup,
		assignments:         bboltx.NewBucketWithDB(db, bucketAssignments, keyCodec, codec.JSON[ConsumerGroupAssignment]()),
		brokerState:         bboltx.NewBucketWithDB(db, bucketBrokerState, keyCodec, codec.JSON[BrokerState]()),
		placements:          bboltx.NewBucketWithDB(db, bucketPlacements, keyCodec, codec.JSON[ShardPlacement]()),
		transactions:        bboltx.NewBucketWithDB(db, bucketTransactions, keyCodec, codec.JSON[TransactionState]()),
		transactionCounters: bboltx.NewBucketWithDB(db, bucketTransactionCounters, keyCodec, codec.JSON[uint64]()),
		aclPolicies:         bboltx.NewBucketWithDB(db, bucketACLPolicies, keyCodec, codec.JSON[ACLPolicy]()),
	}
}
