package ech0

import (
	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func applyEmbeddedRoutingKey(record *store.RecordAppend, routingKey string) {
	if routingKey == "" {
		return
	}
	for index := range record.Headers {
		if record.Headers[index].Key == internalbroker.RoutingKeyHeader {
			record.Headers[index].Value = []byte(routingKey)
			return
		}
	}
	record.Headers = append(record.Headers, store.RecordHeader{Key: internalbroker.RoutingKeyHeader, Value: []byte(routingKey)})
}

func routingKeyFromStore(headers []store.RecordHeader) string {
	for index := range headers {
		header := headers[index]
		if header.Key == internalbroker.RoutingKeyHeader {
			return string(header.Value)
		}
	}
	return ""
}
