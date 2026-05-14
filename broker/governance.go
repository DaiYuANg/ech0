package broker

import (
	"context"
	"strings"

	"github.com/arcgolabs/authx"
	"github.com/lyonbrown4d/ech0/store"
)

const (
	DefaultTenant    = "default"
	DefaultNamespace = "default"
	DefaultPrincipal = "anonymous"

	scopedKeyPrefix = "__tenant/"
)

type contextKey string

const (
	identityContextKey contextKey = "ech0.identity"
)

const (
	authPolicyAllowAll = "ech0.allow_all"
	authAttrTenant     = "tenant"
	authAttrNamespace  = "namespace"
	authAttrClientID   = "client_id"
	authAttrInstance   = "instance"
	authCtxResource    = "resource"
	authCtxResourceID  = "resource_id"
	authCtxTenant      = "tenant"
	authCtxNamespace   = "namespace"
	authCtxClientID    = "client_id"
)

type Identity struct {
	Principal string
	Tenant    string
	Namespace string
	ClientID  string
	Instance  string
}

type AuthRequest struct {
	ClientID  string
	Principal string
	Tenant    string
	Namespace string
	Token     string
}

type ACLAction string

const (
	ACLActionCreate   ACLAction = "create"
	ACLActionDescribe ACLAction = "describe"
	ACLActionProduce  ACLAction = "produce"
	ACLActionConsume  ACLAction = "consume"
	ACLActionCommit   ACLAction = "commit"
	ACLActionAlter    ACLAction = "alter"
	ACLActionDelete   ACLAction = "delete"
	ACLActionTransact ACLAction = "transact"
	ACLActionAdmin    ACLAction = "admin"
)

type ACLResourceType string

const (
	ACLResourceCluster         ACLResourceType = "cluster"
	ACLResourceTenant          ACLResourceType = "tenant"
	ACLResourceNamespace       ACLResourceType = "namespace"
	ACLResourceTopic           ACLResourceType = "topic"
	ACLResourceConsumerGroup   ACLResourceType = "consumer_group"
	ACLResourceTransactionalID ACLResourceType = "transactional_id"
	ACLResourceDirectInbox     ACLResourceType = "direct_inbox"
	ACLResourceAdmin           ACLResourceType = "admin"
)

type ACLResource struct {
	Type      ACLResourceType
	Tenant    string
	Namespace string
	Name      string
}

type QuotaAction string

const (
	QuotaActionCreateTopic QuotaAction = "create_topic"
	QuotaActionProduce     QuotaAction = "produce"
	QuotaActionConsume     QuotaAction = "consume"
	QuotaActionRequest     QuotaAction = "request"
)

type QuotaRequest struct {
	Identity          Identity
	Action            QuotaAction
	Topic             string
	Partitions        uint32
	Records           int
	Bytes             int
	CurrentTopics     int
	CurrentPartitions int
}

type QuotaLimiter interface {
	CheckQuota(context.Context, QuotaRequest) error
}

type UnlimitedQuotaLimiter struct{}

func (UnlimitedQuotaLimiter) CheckQuota(context.Context, QuotaRequest) error { return nil }

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	identity = normalizeIdentity(identity)
	ctx = context.WithValue(ctx, identityContextKey, identity)
	return authx.WithPrincipal(ctx, authPrincipal(identity))
}

func WithTenant(ctx context.Context, tenant string) context.Context {
	identity := identityFromContext(ctx)
	identity.Tenant = tenant
	return WithIdentity(ctx, identity)
}

func WithNamespace(ctx context.Context, namespace string) context.Context {
	identity := identityFromContext(ctx)
	identity.Namespace = namespace
	return WithIdentity(ctx, identity)
}

func WithPrincipal(ctx context.Context, principal string) context.Context {
	identity := identityFromContext(ctx)
	identity.Principal = principal
	return WithIdentity(ctx, identity)
}

func WithClientID(ctx context.Context, clientID string) context.Context {
	identity := identityFromContext(ctx)
	identity.ClientID = clientID
	return WithIdentity(ctx, identity)
}

func identityFromContext(ctx context.Context) Identity {
	if ctx == nil {
		return normalizeIdentity(Identity{})
	}
	identity, ok := ctx.Value(identityContextKey).(Identity)
	if ok {
		return normalizeIdentity(identity)
	}
	principal, ok := authx.PrincipalFromContext(ctx)
	if !ok {
		return normalizeIdentity(Identity{})
	}
	return identityFromPrincipal(principal, Identity{})
}

func normalizeIdentity(identity Identity) Identity {
	identity.Principal = strings.TrimSpace(identity.Principal)
	identity.Tenant = strings.TrimSpace(identity.Tenant)
	identity.Namespace = strings.TrimSpace(identity.Namespace)
	identity.ClientID = strings.TrimSpace(identity.ClientID)
	identity.Instance = strings.TrimSpace(identity.Instance)
	if identity.Principal == "" {
		identity.Principal = DefaultPrincipal
	}
	if identity.Tenant == "" {
		identity.Tenant = DefaultTenant
	}
	if identity.Namespace == "" {
		identity.Namespace = DefaultNamespace
	}
	return identity
}

func (b *Broker) identity(ctx context.Context) Identity {
	identity := identityFromContext(ctx)
	if identity.Tenant == DefaultTenant && identity.Namespace == DefaultNamespace {
		identity.Tenant = b.cfg.Governance.DefaultTenant
		identity.Namespace = b.cfg.Governance.DefaultNamespace
	}
	return normalizeIdentity(identity)
}

func (b *Broker) authenticate(ctx context.Context, req AuthRequest) (Identity, error) {
	if b.auth == nil {
		b.auth = newDefaultAuthEngine(b.logger)
	}
	result, err := b.auth.Check(ctx, req)
	if err != nil {
		return Identity{}, wrapBroker("auth_failed", err, "authenticate client")
	}
	return identityFromAuthResult(req, result), nil
}

func (b *Broker) authorize(ctx context.Context, identity Identity, action ACLAction, resource ACLResource) error {
	if b.auth == nil {
		b.auth = newDefaultAuthEngine(b.logger)
	}
	identity = normalizeIdentity(identity)
	resource.Tenant = nonEmpty(resource.Tenant, identity.Tenant)
	resource.Namespace = nonEmpty(resource.Namespace, identity.Namespace)
	model := authx.AuthorizationModel{
		Principal: authPrincipal(identity),
		Action:    string(action),
		Resource:  authResource(resource),
		Context:   authContext(identity, resource),
	}
	decision, err := b.auth.Can(ctx, model)
	if err != nil {
		return wrapBroker("acl_failed", err, "authorize %s on %s %s", action, resource.Type, resource.Name)
	}
	if !decision.Allowed {
		reason := nonEmpty(decision.Reason, "denied")
		return wrapBroker(
			"acl_denied",
			store.E(store.CodeInvalidArgument, "authorization denied: %s", reason),
			"authorize %s on %s %s",
			action,
			resource.Type,
			resource.Name,
		)
	}
	return nil
}

func (b *Broker) checkQuota(ctx context.Context, req QuotaRequest) error {
	if b.quota == nil {
		b.quota = UnlimitedQuotaLimiter{}
	}
	req.Identity = normalizeIdentity(req.Identity)
	if err := b.quota.CheckQuota(ctx, req); err != nil {
		return wrapBroker("quota_exceeded", err, "check %s quota", req.Action)
	}
	return nil
}

func topicResource(identity Identity, topic string) ACLResource {
	return ACLResource{Type: ACLResourceTopic, Tenant: identity.Tenant, Namespace: identity.Namespace, Name: topic}
}

func groupResource(identity Identity, group string) ACLResource {
	return ACLResource{Type: ACLResourceConsumerGroup, Tenant: identity.Tenant, Namespace: identity.Namespace, Name: group}
}

func txResource(identity Identity, transactionalID string) ACLResource {
	return ACLResource{Type: ACLResourceTransactionalID, Tenant: identity.Tenant, Namespace: identity.Namespace, Name: transactionalID}
}

func directInboxResource(identity Identity, recipient string) ACLResource {
	return ACLResource{Type: ACLResourceDirectInbox, Tenant: identity.Tenant, Namespace: identity.Namespace, Name: recipient}
}
