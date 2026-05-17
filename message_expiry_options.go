package ech0

import (
	"time"

	"github.com/lyonbrown4d/ech0/store"
)

func DefaultMessageTTL(duration time.Duration) TopicOption {
	return func(opts *topicOptions) {
		if duration >= 0 {
			value := durationMillis(duration)
			opts.messageTTLMS = &value
		}
	}
}

func ExpireMessagesToDLQ() TopicOption {
	return func(opts *topicOptions) {
		opts.expiryAction = store.MessageExpiryDLQ
	}
}

func ExpiresAt(value time.Time) PublishOption {
	return func(opts *publishOptions) {
		if !value.IsZero() {
			expiresAt := timeUnixMillis(value)
			opts.expiresAt = &expiresAt
		}
	}
}

func MessageTTL(duration time.Duration) PublishOption {
	return func(opts *publishOptions) {
		if duration >= 0 {
			expiresAt := addMillis(store.NowMS(), durationMillis(duration))
			opts.expiresAt = &expiresAt
		}
	}
}

func addMillis(value, delta uint64) uint64 {
	if delta > ^uint64(0)-value {
		return ^uint64(0)
	}
	return value + delta
}

func timeUnixMillis(value time.Time) uint64 {
	millis := value.UnixMilli()
	if millis <= 0 {
		return 0
	}
	return uint64(millis)
}
