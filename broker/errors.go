package broker

import (
	"fmt"

	"github.com/DaiYuANg/ech0/store"
	"github.com/samber/oops"
)

func wrapBroker(code string, err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return oops.In("broker").Code(code).Wrapf(err, "%s", fmt.Sprintf(format, args...))
}

func wrapBrokerStore(err error, format string) error {
	if err == nil {
		return nil
	}
	return oops.In("broker").Code(string(store.ErrorCode(err))).Wrapf(err, "%s", format)
}

func brokerStoreError(code store.Code, format string, args ...any) error {
	return wrapBrokerStore(store.E(code, format, args...), "broker store error")
}
