package store

import "fmt"

type Code string

const (
	CodeInvalidArgument   Code = "invalid_argument"
	CodeTopicExists       Code = "topic_already_exists"
	CodeTopicNotFound     Code = "topic_not_found"
	CodePartitionNotFound Code = "partition_not_found"
	CodeCodec             Code = "codec_error"
	CodeNotLeader         Code = "not_leader"
	CodeUnavailable       Code = "unavailable"
)

type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func E(code Code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return CodeUnavailable
}
