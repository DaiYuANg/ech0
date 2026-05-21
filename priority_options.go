package ech0

import "github.com/lyonbrown4d/ech0/store"

func PriorityRange(minPriority, maxPriority, defaultPriority uint8) TopicOption {
	return func(opts *topicOptions) {
		opts.priorityPolicy = &store.TopicPriorityPolicy{
			Enabled: true,
			Min:     minPriority,
			Max:     maxPriority,
			Default: defaultPriority,
		}
	}
}

func Priority(priority uint8) PublishOption {
	return func(opts *publishOptions) {
		opts.priority = &priority
	}
}
