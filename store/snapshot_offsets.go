package store

import (
	"strconv"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

func restoreMemoryOffsetStates(
	snapshotStates collectionlist.List[ConsumerOffsetState],
	snapshotOffsets collectionmapping.Map[string, uint64],
) *collectionmapping.Map[string, ConsumerOffsetState] {
	states := collectionmapping.NewMap[string, ConsumerOffsetState]()
	snapshotOffsets.Range(func(key string, offset uint64) bool {
		if state, ok := consumerOffsetStateFromKey(key, offset); ok {
			states.Set(key, state)
		}
		return true
	})
	snapshotStates.Range(func(_ int, state ConsumerOffsetState) bool {
		if validateConsumerOffsetState(state) == nil {
			states.Set(offsetKey(state.Consumer, state.TopicPartition()), state)
		}
		return true
	})
	return states
}

func consumerOffsetStateFromKey(key string, offset uint64) (ConsumerOffsetState, bool) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 3 {
		return ConsumerOffsetState{}, false
	}
	partition, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return ConsumerOffsetState{}, false
	}
	return ConsumerOffsetState{Consumer: parts[0], Topic: parts[1], Partition: uint32(partition), NextOffset: offset}, true
}
