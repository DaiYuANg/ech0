package protocol

type TopicPriorityPolicy struct {
	Enabled bool  `json:"enabled"`
	Min     uint8 `json:"min"`
	Max     uint8 `json:"max"`
	Default uint8 `json:"default"`
}
