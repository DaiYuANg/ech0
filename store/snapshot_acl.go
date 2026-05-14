package store

import (
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func restoreMemoryACLPolicies(snapshotPolicies collectionlist.List[ACLPolicy]) *collectionmapping.Map[string, ACLPolicy] {
	policies := collectionmapping.NewMapWithCapacity[string, ACLPolicy](snapshotPolicies.Len())
	snapshotPolicies.Range(func(_ int, policy ACLPolicy) bool {
		if validateACLPolicy(policy) == nil {
			policies.Set(policy.PolicyID, cloneACLPolicy(policy))
		}
		return true
	})
	return policies
}
