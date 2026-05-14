package broker

import (
	"strings"

	"github.com/lyonbrown4d/ech0/store"
)

func visibleConsumerOffsetState(identity Identity, state *store.ConsumerOffsetState) *store.ConsumerOffsetState {
	if state == nil {
		return nil
	}
	out := *state
	out.Consumer = visibleName(identity, "consumer", out.Consumer)
	out.Topic = visibleTopicName(identity, out.Topic)
	return &out
}

func visibleGroupOffsetState(identity Identity, state *store.ConsumerOffsetState) *store.ConsumerOffsetState {
	if state == nil {
		return nil
	}
	out := *state
	group, ok := groupNameFromConsumer(out.Consumer)
	if ok {
		out.Consumer = visibleName(identity, "group", group)
	}
	out.Topic = visibleTopicName(identity, out.Topic)
	return &out
}

func groupNameFromConsumer(consumer string) (string, bool) {
	const prefix = "__group_offset/"
	return strings.CutPrefix(consumer, prefix)
}
