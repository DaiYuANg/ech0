// Package store defines broker storage models and implementations.
package store

import (
	"errors"
	"fmt"

	"github.com/samber/oops"
)

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
	return oops.
		In("store").
		Code(code).
		Wrap(&Error{Code: code, Message: fmt.Sprintf(format, args...)})
}

func wrapExternal(err error, format string) error {
	if err == nil {
		return nil
	}
	return oops.
		In("store").
		Code(CodeUnavailable).
		Wrapf(err, "%s", format)
}

func ErrorCode(err error) Code {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeUnavailable
}
