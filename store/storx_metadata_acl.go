package store

import (
	"context"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
)

func (s *StorxMetadataStore) SaveACLPolicy(policy ACLPolicy) error {
	if err := validateACLPolicy(policy); err != nil {
		return err
	}
	return wrapExternal(s.aclPolicies.Put(context.Background(), policy.PolicyID, cloneACLPolicy(policy)), "save acl policy")
}

func (s *StorxMetadataStore) LoadACLPolicy(policyID string) (*ACLPolicy, error) {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return nil, E(CodeInvalidArgument, "policy_id is required")
	}
	policy, ok, err := s.aclPolicies.Get(context.Background(), policyID)
	if err != nil {
		return nil, wrapExternal(err, "load acl policy")
	}
	if !ok {
		var absent *ACLPolicy
		return absent, nil
	}
	policy = cloneACLPolicy(policy)
	return &policy, nil
}

func (s *StorxMetadataStore) ListACLPolicies(filter ACLPolicyFilter) ([]ACLPolicy, error) {
	out := collectionlist.NewList[ACLPolicy]()
	err := s.aclPolicies.Walk(context.Background(), func(entry bboltx.Entry[string, ACLPolicy]) error {
		if aclPolicyMatchesFilter(entry.Value, filter) {
			out.Add(cloneACLPolicy(entry.Value))
		}
		return nil
	})
	if err != nil {
		return nil, wrapExternal(err, "list acl policies")
	}
	return sortACLPolicies(out.Values()), nil
}

func (s *StorxMetadataStore) DeleteACLPolicy(policyID string) error {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return E(CodeInvalidArgument, "policy_id is required")
	}
	return wrapExternal(s.aclPolicies.Delete(context.Background(), policyID), "delete acl policy")
}
