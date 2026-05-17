package store

type MessageExpiryAction string

const (
	MessageExpiryDelete MessageExpiryAction = "delete"
	MessageExpiryDLQ    MessageExpiryAction = "dlq"
)
