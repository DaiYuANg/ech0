package broker

import (
	"context"
	"strings"

	"github.com/arcgolabs/authx"
	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/lo"
)

const (
	authPolicyMetadataACL     = "ech0.metadata_acl"
	authReasonPolicyRequired  = "policy_required"
	authReasonPolicyDeny      = "policy_deny"
	authReasonInvalidMetadata = "invalid_acl_metadata"
)

func newMetadataACLAuthorizer(b *Broker) authx.Authorizer {
	return authx.AuthorizerFunc(func(ctx context.Context, input authx.AuthorizationModel) (authx.Decision, error) {
		return b.authorizeFromMetadata(ctx, input)
	})
}

func (b *Broker) authorizeFromMetadata(_ context.Context, input authx.AuthorizationModel) (authx.Decision, error) {
	if b == nil || b.meta == nil {
		return authx.Decision{Allowed: true, PolicyID: authPolicyAllowAll}, nil
	}
	request := aclRequestFromAuthModel(input)
	policies, err := b.meta.ListACLPolicies(store.ACLPolicyFilter{})
	if err != nil {
		wrapped := wrapBroker("acl_policy_load_failed", err, "load acl policies")
		return authx.Decision{Allowed: false, Reason: authReasonInvalidMetadata, PolicyID: authPolicyMetadataACL}, wrapped
	}
	scoped := scopedACLPolicies(policies, request.Resource)
	if len(scoped) == 0 {
		return authx.Decision{Allowed: true, PolicyID: authPolicyAllowAll}, nil
	}
	for i := range scoped {
		policy := scoped[i]
		if !metadataACLPolicyMatches(policy, request) {
			continue
		}
		decision := authx.Decision{Allowed: policy.Effect == store.ACLPolicyEffectAllow, PolicyID: policy.PolicyID}
		if !decision.Allowed {
			decision.Reason = authReasonPolicyDeny
		}
		return decision, nil
	}
	return authx.Decision{Allowed: false, Reason: authReasonPolicyRequired, PolicyID: authPolicyMetadataACL}, nil
}

type aclAuthorizationRequest struct {
	Identity Identity
	Action   ACLAction
	Resource ACLResource
}

func aclRequestFromAuthModel(input authx.AuthorizationModel) aclAuthorizationRequest {
	identity := identityFromPrincipal(input.Principal, Identity{})
	resource := ACLResource{
		Type:      ACLResourceType(stringDetail(input.Context, authCtxResource, "")),
		Tenant:    stringDetail(input.Context, authCtxTenant, identity.Tenant),
		Namespace: stringDetail(input.Context, authCtxNamespace, identity.Namespace),
		Name:      stringDetail(input.Context, authCtxResourceID, ""),
	}
	if resource.Type == "" {
		resource.Type = parseAuthResourceType(input.Resource)
	}
	resource.Tenant = nonEmpty(resource.Tenant, identity.Tenant)
	resource.Namespace = nonEmpty(resource.Namespace, identity.Namespace)
	return aclAuthorizationRequest{Identity: identity, Action: ACLAction(input.Action), Resource: resource}
}

func scopedACLPolicies(policies []store.ACLPolicy, resource ACLResource) []store.ACLPolicy {
	out := collectionlist.NewList[store.ACLPolicy]()
	for i := range policies {
		policy := policies[i]
		if aclFieldMatches(policy.Tenant, resource.Tenant) && aclFieldMatches(policy.Namespace, resource.Namespace) {
			out.Add(policy)
		}
	}
	return out.Values()
}

func metadataACLPolicyMatches(policy store.ACLPolicy, request aclAuthorizationRequest) bool {
	return aclFieldMatches(policy.Principal, request.Identity.Principal) &&
		aclFieldMatches(policy.ResourceType, string(request.Resource.Type)) &&
		aclFieldMatches(policy.ResourceName, request.Resource.Name) &&
		aclActionMatches(policy.Actions, string(request.Action))
}

func aclFieldMatches(policyValue, requestValue string) bool {
	policyValue = strings.TrimSpace(policyValue)
	requestValue = strings.TrimSpace(requestValue)
	return policyValue == "" || policyValue == aclPolicyWildcard || policyValue == requestValue
}

func aclActionMatches(actions []string, action string) bool {
	if len(actions) == 0 {
		return true
	}
	return lo.SomeBy(actions, func(candidate string) bool {
		return aclFieldMatches(candidate, action)
	})
}

func parseAuthResourceType(resource string) ACLResourceType {
	before, _, ok := strings.Cut(resource, ":")
	if !ok {
		return ""
	}
	return ACLResourceType(before)
}
