package store

type TopicOrderingPolicy string

const (
	TopicOrderingNone       TopicOrderingPolicy = ""
	TopicOrderingPartition  TopicOrderingPolicy = "partition"
	TopicOrderingKey        TopicOrderingPolicy = "key"
	TopicOrderingRoutingKey TopicOrderingPolicy = "routing_key"
)
