package store

import (
	"cmp"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
)

func (s *MemoryStore) SaveACLPolicy(policy ACLPolicy) error {
	if err := validateACLPolicy(policy); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aclPolicies.Set(policy.PolicyID, cloneACLPolicy(policy))
	return nil
}

func (s *MemoryStore) LoadACLPolicy(policyID string) (*ACLPolicy, error) {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return nil, E(CodeInvalidArgument, "policy_id is required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	policy, ok := s.aclPolicies.Get(policyID)
	if !ok {
		var absent *ACLPolicy
		return absent, nil
	}
	policy = cloneACLPolicy(policy)
	return &policy, nil
}

func (s *MemoryStore) ListACLPolicies(filter ACLPolicyFilter) ([]ACLPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := collectionlist.NewList[ACLPolicy]()
	s.aclPolicies.Range(func(_ string, policy ACLPolicy) bool {
		if aclPolicyMatchesFilter(policy, filter) {
			out.Add(cloneACLPolicy(policy))
		}
		return true
	})
	return sortACLPolicies(out.Values()), nil
}

func (s *MemoryStore) DeleteACLPolicy(policyID string) error {
	policyID = strings.TrimSpace(policyID)
	if policyID == "" {
		return E(CodeInvalidArgument, "policy_id is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aclPolicies.Delete(policyID)
	return nil
}

func validateACLPolicy(policy ACLPolicy) error {
	if strings.TrimSpace(policy.PolicyID) == "" {
		return E(CodeInvalidArgument, "policy_id is required")
	}
	switch policy.Effect {
	case ACLPolicyEffectAllow, ACLPolicyEffectDeny:
	default:
		return E(CodeInvalidArgument, "acl policy %s has invalid effect %q", policy.PolicyID, policy.Effect)
	}
	return nil
}

func aclPolicyMatchesFilter(policy ACLPolicy, filter ACLPolicyFilter) bool {
	return stringFilterMatch(policy.Tenant, filter.Tenant) &&
		stringFilterMatch(policy.Namespace, filter.Namespace) &&
		stringFilterMatch(policy.Principal, filter.Principal) &&
		stringFilterMatch(policy.ResourceType, filter.ResourceType) &&
		stringFilterMatch(policy.ResourceName, filter.ResourceName)
}

func stringFilterMatch(value, filter string) bool {
	filter = strings.TrimSpace(filter)
	return filter == "" || strings.TrimSpace(value) == filter
}

func sortACLPolicies(policies []ACLPolicy) []ACLPolicy {
	return collectionlist.NewList(policies...).
		Sort(func(left, right ACLPolicy) int {
			if left.Priority != right.Priority {
				return cmp.Compare(right.Priority, left.Priority)
			}
			if left.Effect != right.Effect {
				if left.Effect == ACLPolicyEffectDeny {
					return -1
				}
				if right.Effect == ACLPolicyEffectDeny {
					return 1
				}
			}
			return cmp.Compare(left.PolicyID, right.PolicyID)
		}).
		Values()
}

func cloneACLPolicy(policy ACLPolicy) ACLPolicy {
	policy.Actions = collectionlist.NewList(policy.Actions...).Values()
	return policy
}
