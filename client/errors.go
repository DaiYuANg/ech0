package client

import (
	"errors"

	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/samber/oops"
)

type BrokerError struct {
	Code    string
	Message string
}

func (e *BrokerError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code == "" {
		return e.Message
	}
	if e.Message == "" {
		return e.Code
	}
	return "ech0 broker error " + e.Code + ": " + e.Message
}

func IsBrokerError(err error, code string) bool {
	var brokerErr *BrokerError
	if !errors.As(err, &brokerErr) {
		return false
	}
	return brokerErr.Code == code
}

func brokerErrorFromProtocol(resp protocol.ErrorResponse) *BrokerError {
	return &BrokerError{Code: resp.Code, Message: resp.Message}
}

func wrap(code string, err error, message string) error {
	if err == nil {
		return nil
	}
	return oops.In("client").Code(code).Wrapf(err, "%s", message)
}
