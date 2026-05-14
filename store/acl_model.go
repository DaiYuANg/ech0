package store

type ACLPolicyEffect string

const (
	ACLPolicyEffectAllow ACLPolicyEffect = "allow"
	ACLPolicyEffectDeny  ACLPolicyEffect = "deny"
)

type ACLPolicy struct {
	PolicyID     string          `json:"policy_id"`
	Tenant       string          `json:"tenant"`
	Namespace    string          `json:"namespace"`
	Principal    string          `json:"principal"`
	ResourceType string          `json:"resource_type"`
	ResourceName string          `json:"resource_name"`
	Actions      []string        `json:"actions"`
	Effect       ACLPolicyEffect `json:"effect"`
	Priority     int             `json:"priority"`
	UpdatedAtMS  uint64          `json:"updated_at_ms"`
}

type ACLPolicyFilter struct {
	Tenant       string
	Namespace    string
	Principal    string
	ResourceType string
	ResourceName string
}
