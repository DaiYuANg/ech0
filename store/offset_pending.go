package store

import "slices"

func NormalizeConsumerOffsetState(state ConsumerOffsetState) ConsumerOffsetState {
	if len(state.PendingOffsets) == 0 {
		return state
	}
	pending := append([]uint64(nil), state.PendingOffsets...)
	slices.Sort(pending)
	out := pending[:0]
	var previous uint64
	for index, offset := range pending {
		if offset < state.NextOffset {
			continue
		}
		if index > 0 && offset == previous {
			continue
		}
		out = append(out, offset)
		previous = offset
	}
	state.PendingOffsets = append([]uint64(nil), out...)
	return state
}
