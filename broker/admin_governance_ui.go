package broker

import (
	"strconv"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/gofiber/fiber/v3"
)

const governanceACLPreviewLimit = 5

type governanceView struct {
	Identity       Identity
	Quota          QuotaSummary
	QuotaError     string
	Auth           governanceAuthView
	TenantDefaults []governanceTenantDefaultView
	ACLPolicies    []ACLPolicy
	ACLPolicyCount int
	ACLError       string
}

type governanceAuthView struct {
	Enabled          bool
	AllowAnonymous   bool
	StaticTokenCount int
	StaticTokens     []governanceStaticTokenView
}

type governanceStaticTokenView struct {
	Principal string
	Tenant    string
	Namespace string
	ClientID  string
	Instance  string
}

type governanceTenantDefaultView struct {
	Tenant              string
	Namespace           string
	RetentionMaxBytes   uint64
	RetentionMS         string
	MessageTTLMS        string
	MessageExpiryAction string
	DelayEnabled        string
	RetryMaxAttempts    uint32
	DeadLetterTopic     string
}

func (s *AdminServer) uiGovernance(c fiber.Ctx) error {
	identity := identityFromContext(c.Context())
	view := governanceView{
		Identity:       identity,
		Auth:           governanceAuthFromConfig(s.effectiveGovernanceConfig().Auth),
		TenantDefaults: governanceTenantDefaultsFromConfig(s.effectiveGovernanceConfig().TenantDefaults),
	}
	quota, err := s.broker.QuotaSummaryFor(c.Context())
	if err != nil {
		view.QuotaError = err.Error()
	} else {
		view.Quota = quota
	}
	policies, err := s.broker.ListACLPolicies(c.Context(), ACLPolicyFilter{
		Tenant:    identity.Tenant,
		Namespace: identity.Namespace,
	})
	if err != nil {
		view.ACLError = err.Error()
	} else {
		view.ACLPolicyCount = len(policies)
		view.ACLPolicies = previewACLPolicies(policies)
	}
	return adminRender(c, "admin_templates/governance", view)
}

func (s *AdminServer) effectiveGovernanceConfig() GovernanceConfig {
	if s.broker != nil {
		return s.broker.cfg.Governance
	}
	return s.cfg.Governance
}

func governanceAuthFromConfig(cfg AuthConfig) governanceAuthView {
	tokens := collectionlist.NewListWithCapacity[governanceStaticTokenView](len(cfg.StaticTokens))
	for _, token := range cfg.StaticTokens {
		tokens.Add(governanceStaticTokenView{
			Principal: token.Principal,
			Tenant:    token.Tenant,
			Namespace: token.Namespace,
			ClientID:  token.ClientID,
			Instance:  token.Instance,
		})
	}
	return governanceAuthView{
		Enabled:          cfg.Enabled,
		AllowAnonymous:   cfg.AllowAnonymous,
		StaticTokenCount: len(cfg.StaticTokens),
		StaticTokens:     tokens.Values(),
	}
}

func governanceTenantDefaultsFromConfig(defaults []TenantDefaultsConfig) []governanceTenantDefaultView {
	out := collectionlist.NewListWithCapacity[governanceTenantDefaultView](len(defaults))
	for index := range defaults {
		item := &defaults[index]
		out.Add(governanceTenantDefaultView{
			Tenant:              item.Tenant,
			Namespace:           item.Namespace,
			RetentionMaxBytes:   item.RetentionMaxBytes,
			RetentionMS:         optionalUint64Value(item.RetentionMS),
			MessageTTLMS:        optionalUint64Value(item.MessageTTLMS),
			MessageExpiryAction: string(item.MessageExpiryAction),
			DelayEnabled:        optionalBoolValue(item.DelayEnabled),
			RetryMaxAttempts:    item.RetryPolicy.MaxAttempts,
			DeadLetterTopic:     item.DeadLetterTopic,
		})
	}
	return out.Values()
}

func previewACLPolicies(policies []ACLPolicy) []ACLPolicy {
	limit := min(governanceACLPreviewLimit, len(policies))
	out := collectionlist.NewListWithCapacity[ACLPolicy](limit)
	for index := range policies {
		if index >= limit {
			break
		}
		out.Add(policies[index])
	}
	return out.Values()
}

func optionalUint64Value(value *uint64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatUint(*value, 10)
}

func optionalBoolValue(value *bool) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatBool(*value)
}
