package broker

import "github.com/lyonbrown4d/ech0/store"

const RoutingKeyHeader = "x-ech0-routing-key"

func recordRoutingKey(record store.Record) string {
	return routingKeyFromHeaders(record.Headers)
}

func appendRoutingKey(record store.RecordAppend) string {
	return routingKeyFromHeaders(record.Headers)
}

func routingKeyFromHeaders(headers []store.RecordHeader) string {
	for index := range headers {
		header := headers[index]
		if header.Key == RoutingKeyHeader {
			return string(header.Value)
		}
	}
	return ""
}

func applyRoutingKey(record *store.RecordAppend, routingKey string) {
	if routingKey == "" {
		return
	}
	upsertHeader(&record.Headers, RoutingKeyHeader, routingKey)
}

func applyPartitioningRoutingKey(record *store.RecordAppend, partitioning PublishPartitioning) {
	applyRoutingKey(record, partitioning.RoutingKey)
}

func recordsWithPartitioningRoutingKey(partitioning PublishPartitioning, records []store.RecordAppend) []store.RecordAppend {
	if partitioning.RoutingKey == "" || len(records) == 0 {
		return records
	}
	out := make([]store.RecordAppend, len(records))
	for index := range records {
		out[index] = records[index]
		applyPartitioningRoutingKey(&out[index], partitioning)
	}
	return out
}

func withResolvedRoutingKey(partitioning PublishPartitioning, records []store.RecordAppend) PublishPartitioning {
	if partitioning.Mode != PartitionRoutingKeyHash || partitioning.RoutingKey != "" {
		return partitioning
	}
	partitioning.RoutingKey = partitionRoutingKey(partitioning, records)
	return partitioning
}

func partitionRoutingKey(partitioning PublishPartitioning, records []store.RecordAppend) string {
	if partitioning.RoutingKey != "" {
		return partitioning.RoutingKey
	}
	for index := range records {
		routingKey := appendRoutingKey(records[index])
		if routingKey != "" {
			return routingKey
		}
	}
	return ""
}
