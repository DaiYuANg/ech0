package broker

import (
	"context"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	ACLPolicyEffectAllow ACLPolicyEffect = "allow"
	ACLPolicyEffectDeny  ACLPolicyEffect = "deny"

	aclPolicyWildcard = "*"
)

type ACLPolicyEffect string

type ACLPolicy struct {
	PolicyID     string          `json:"policy_id,omitempty"`
	Tenant       string          `json:"tenant,omitempty"`
	Namespace    string          `json:"namespace,omitempty"`
	Principal    string          `json:"principal,omitempty"`
	ResourceType ACLResourceType `json:"resource_type,omitempty"`
	ResourceName string          `json:"resource_name,omitempty"`
	Actions      []ACLAction     `json:"actions,omitempty"`
	Effect       ACLPolicyEffect `json:"effect,omitempty"`
	Priority     int             `json:"priority,omitempty"`
	UpdatedAtMS  uint64          `json:"updated_at_ms,omitempty"`
}

type ACLPolicyFilter struct {
	Tenant       string          `json:"tenant,omitempty"`
	Namespace    string          `json:"namespace,omitempty"`
	Principal    string          `json:"principal,omitempty"`
	ResourceType ACLResourceType `json:"resource_type,omitempty"`
	ResourceName string          `json:"resource_name,omitempty"`
}

type deleteACLPolicyCommand struct {
	PolicyID string `json:"policy_id"`
}

type deleteACLPolicyResult struct {
	PolicyID string `json:"policy_id"`
}

func (b *Broker) UpsertACLPolicy(ctx context.Context, policy ACLPolicy) (ACLPolicy, error) {
	identity := b.identity(ctx)
	policy = normalizeACLPolicy(identity, policy)
	if err := validateBrokerACLPolicy(policy); err != nil {
		return ACLPolicy{}, err
	}
	if err := b.authorize(ctx, identity, ACLActionAdmin, aclPolicyAdminResource(identity)); err != nil {
		return ACLPolicy{}, err
	}
	stored, err := routeMetadataCommand(ctx, b, raftCommandUpsertACLPolicy, brokerACLPolicyToStore(policy), b.applyUpsertACLPolicy)
	if err != nil {
		return ACLPolicy{}, err
	}
	return storeACLPolicyToBroker(stored), nil
}

func (b *Broker) ListACLPolicies(ctx context.Context, filter ACLPolicyFilter) ([]ACLPolicy, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAdmin, aclPolicyAdminResource(identity)); err != nil {
		return nil, err
	}
	policies, err := b.meta.ListACLPolicies(brokerACLPolicyFilterToStore(filter))
	if err != nil {
		return nil, wrapBroker("list_acl_policies_failed", err, "list acl policies")
	}
	out := collectionlist.NewListWithCapacity[ACLPolicy](len(policies))
	for i := range policies {
		out.Add(storeACLPolicyToBroker(policies[i]))
	}
	return out.Values(), nil
}

func (b *Broker) DeleteACLPolicy(ctx context.Context, policyID string) error {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionAdmin, aclPolicyAdminResource(identity)); err != nil {
		return err
	}
	_, err := routeMetadataCommand(ctx, b, raftCommandDeleteACLPolicy, deleteACLPolicyCommand{PolicyID: strings.TrimSpace(policyID)}, b.applyDeleteACLPolicy)
	return err
}

func (b *Broker) applyUpsertACLPolicy(_ context.Context, policy store.ACLPolicy) (store.ACLPolicy, error) {
	if err := b.meta.SaveACLPolicy(policy); err != nil {
		return store.ACLPolicy{}, wrapBroker("save_acl_policy_failed", err, "save acl policy %s", policy.PolicyID)
	}
	return policy, nil
}

func (b *Broker) applyDeleteACLPolicy(_ context.Context, req deleteACLPolicyCommand) (deleteACLPolicyResult, error) {
	req.PolicyID = strings.TrimSpace(req.PolicyID)
	if req.PolicyID == "" {
		return deleteACLPolicyResult{}, brokerStoreError(store.CodeInvalidArgument, "policy_id is required")
	}
	if err := b.meta.DeleteACLPolicy(req.PolicyID); err != nil {
		return deleteACLPolicyResult{}, wrapBroker("delete_acl_policy_failed", err, "delete acl policy %s", req.PolicyID)
	}
	return deleteACLPolicyResult(req), nil
}
