package store

const (
	DefaultTopicPriorityMin     uint8 = 0
	DefaultTopicPriorityMax     uint8 = 9
	DefaultTopicPriorityDefault uint8 = 0
)

type TopicPriorityPolicy struct {
	Enabled bool  `json:"enabled" toml:"enabled"`
	Min     uint8 `json:"min"     toml:"min"`
	Max     uint8 `json:"max"     toml:"max"`
	Default uint8 `json:"default" toml:"default"`
}

func NormalizeTopicPriorityPolicy(policy TopicPriorityPolicy) TopicPriorityPolicy {
	if !policy.Enabled {
		return TopicPriorityPolicy{}
	}
	if policy.Max == 0 {
		policy.Max = DefaultTopicPriorityMax
	}
	return policy
}
