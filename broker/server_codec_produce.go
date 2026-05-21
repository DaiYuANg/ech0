package broker

import (
	"errors"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/store"
)

func batchRecordsFromProtocol(req protocol.ProduceBatchRequest) ([]store.RecordAppend, error) {
	if len(req.Payloads) == 0 && len(req.Records) == 0 {
		return nil, errors.New("produce_batch requires payloads or records")
	}
	if len(req.Payloads) > 0 && len(req.Records) > 0 {
		return nil, errors.New("produce_batch must provide only one of payloads or records")
	}
	if len(req.Records) > 0 {
		return batchRecordItemsFromProtocol(req.Records), nil
	}
	return batchPayloadsFromProtocol(req.Payloads), nil
}

func batchRecordItemsFromProtocol(records []protocol.ProduceBatchRecord) []store.RecordAppend {
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for index := range records {
		out.Add(recordItemFromProtocol(records[index]))
	}
	return out.Values()
}

func recordItemFromProtocol(record protocol.ProduceBatchRecord) store.RecordAppend {
	appendRecord := store.NewRecordAppend(record.Payload)
	appendRecord.Key = append([]byte(nil), record.Key...)
	appendRecord.Headers = storeHeadersFromProtocol(record.Headers)
	applyRoutingKey(&appendRecord, record.RoutingKey)
	appendRecord.ExpiresAtMS = cloneUint64Ptr(record.ExpiresAtMS)
	if record.Tombstone {
		appendRecord.Attributes |= store.RecordAttributeTombstone
	}
	return appendRecord
}

func batchPayloadsFromProtocol(payloads [][]byte) []store.RecordAppend {
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(payloads))
	for index := range payloads {
		out.Add(store.NewRecordAppend(payloads[index]))
	}
	return out.Values()
}

func batchRecordsWithRequestRoutingKey(req protocol.ProduceBatchRequest, records []store.RecordAppend) []store.RecordAppend {
	if req.RoutingKey == "" {
		return records
	}
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for index := range records {
		record := records[index]
		applyRoutingKey(&record, req.RoutingKey)
		out.Add(record)
	}
	return out.Values()
}

func fanoutResultToProtocol(result FanoutResult) protocol.ProduceFanoutResponse {
	out := collectionlist.NewListWithCapacity[protocol.ProduceFanoutRecordResponse](len(result.Records))
	for index := range result.Records {
		record := &result.Records[index]
		out.Add(protocol.ProduceFanoutRecordResponse{
			Partition:  record.Partition,
			Offset:     record.Record.Offset,
			NextOffset: record.NextOffset,
		})
	}
	return protocol.ProduceFanoutResponse{Records: out.Values()}
}
