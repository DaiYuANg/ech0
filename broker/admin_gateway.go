package broker

import (
	"context"
	"encoding/base64"
	"strconv"
	"strings"
	"unicode/utf8"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

type gatewayHeader struct {
	Key         string `json:"key"`
	Value       string `json:"value,omitempty"`
	ValueBase64 string `json:"value_base64,omitempty"`
}

type gatewayProduceInput struct {
	Topic string `path:"topic"`
	Body  struct {
		Payload       string          `json:"payload,omitempty"`
		PayloadBase64 string          `json:"payload_base64,omitempty"`
		Key           string          `json:"key,omitempty"`
		KeyBase64     string          `json:"key_base64,omitempty"`
		Headers       []gatewayHeader `json:"headers,omitempty"`
		RoutingKey    string          `json:"routing_key,omitempty"`
		Partition     *uint32         `json:"partition,omitempty"`
		Priority      *uint8          `json:"priority,omitempty"`
		Tombstone     bool            `json:"tombstone,omitempty"`
		ExpiresAtMS   *uint64         `json:"expires_at_ms,omitempty"`
	} `json:"body"`
}

type gatewayProduceOutput struct {
	Body struct {
		Topic      string `json:"topic"`
		Partition  uint32 `json:"partition"`
		Offset     uint64 `json:"offset"`
		NextOffset uint64 `json:"next_offset"`
	} `json:"body"`
}

type gatewayFetchInput struct {
	Topic      string `path:"topic"`
	Partition  uint32 `path:"partition"`
	Consumer   string `query:"consumer"    validate:"required"`
	Offset     string `query:"offset"`
	MaxRecords int    `query:"max_records"`
	Isolation  string `query:"isolation"`
}

type gatewayFetchOutput struct {
	Body struct {
		Topic          string                  `json:"topic"`
		Partition      uint32                  `json:"partition"`
		Records        []gatewayRecordResponse `json:"records"`
		NextOffset     uint64                  `json:"next_offset"`
		HighWatermark  *uint64                 `json:"high_watermark,omitempty"`
		LowWatermark   *uint64                 `json:"low_watermark,omitempty"`
		LogStartOffset uint64                  `json:"log_start_offset"`
	} `json:"body"`
}

type gatewayRecordResponse struct {
	Offset        uint64          `json:"offset"`
	TimestampMS   uint64          `json:"timestamp_ms"`
	RoutingKey    string          `json:"routing_key,omitempty"`
	KeyBase64     string          `json:"key_base64,omitempty"`
	Headers       []gatewayHeader `json:"headers,omitempty"`
	Tombstone     bool            `json:"tombstone,omitempty"`
	ExpiresAtMS   *uint64         `json:"expires_at_ms,omitempty"`
	Payload       string          `json:"payload,omitempty"`
	PayloadBase64 string          `json:"payload_base64"`
	NextOffset    uint64          `json:"next_offset"`
}

type gatewayCommitInput struct {
	Topic     string `path:"topic"`
	Partition uint32 `path:"partition"`
	Body      struct {
		Consumer   string `json:"consumer"           validate:"required"`
		NextOffset uint64 `json:"next_offset"`
		Metadata   string `json:"metadata,omitempty"`
	} `json:"body"`
}

type gatewayCommitOutput struct {
	Body struct {
		Topic      string `json:"topic"`
		Partition  uint32 `json:"partition"`
		Consumer   string `json:"consumer"`
		NextOffset uint64 `json:"next_offset"`
		Metadata   string `json:"metadata,omitempty"`
	} `json:"body"`
}

func (s *AdminServer) apiGatewayProduce(ctx context.Context, in *gatewayProduceInput) (*gatewayProduceOutput, error) {
	record, err := gatewayRecordAppend(in.Body.Payload, in.Body.PayloadBase64, in.Body.Key, in.Body.KeyBase64, in.Body.Headers)
	if err != nil {
		return nil, err
	}
	record.ExpiresAtMS = cloneUint64Ptr(in.Body.ExpiresAtMS)
	if in.Body.Tombstone {
		record.Attributes |= store.RecordAttributeTombstone
	}
	if in.Body.Priority != nil {
		applyPriority(&record, *in.Body.Priority)
	}
	result, err := s.broker.PublishRecord(ctx, in.Topic, gatewayPartitioning(in), record)
	if err != nil {
		return nil, err
	}
	out := &gatewayProduceOutput{}
	out.Body.Topic = in.Topic
	out.Body.Partition = result.Partition
	out.Body.Offset = result.Record.Offset
	out.Body.NextOffset = result.Record.Offset + 1
	return out, nil
}

func (s *AdminServer) apiGatewayFetch(ctx context.Context, in *gatewayFetchInput) (*gatewayFetchOutput, error) {
	offsetValue, hasOffset, err := gatewayFetchOffset(in.Offset)
	if err != nil {
		return nil, err
	}
	var offset *uint64
	if hasOffset {
		offset = &offsetValue
	}
	poll, err := s.broker.FetchWithIsolation(ctx, in.Consumer, in.Topic, in.Partition, offset, in.MaxRecords, gatewayFetchIsolation(in.Isolation))
	if err != nil {
		return nil, err
	}
	out := &gatewayFetchOutput{}
	out.Body.Topic = in.Topic
	out.Body.Partition = in.Partition
	out.Body.Records = gatewayRecords(poll.Records)
	out.Body.NextOffset = poll.NextOffset
	out.Body.HighWatermark = poll.HighWatermark
	out.Body.LowWatermark = poll.LowWatermark
	out.Body.LogStartOffset = poll.LogStartOffset
	return out, nil
}

func (s *AdminServer) apiGatewayCommit(ctx context.Context, in *gatewayCommitInput) (*gatewayCommitOutput, error) {
	err := s.broker.CommitOffsetWithMetadata(ctx, in.Body.Consumer, in.Topic, in.Partition, in.Body.NextOffset, in.Body.Metadata)
	if err != nil {
		return nil, err
	}
	out := &gatewayCommitOutput{}
	out.Body.Topic = in.Topic
	out.Body.Partition = in.Partition
	out.Body.Consumer = in.Body.Consumer
	out.Body.NextOffset = in.Body.NextOffset
	out.Body.Metadata = in.Body.Metadata
	return out, nil
}

func gatewayRecordAppend(payload, payloadBase64, key, keyBase64 string, headers []gatewayHeader) (store.RecordAppend, error) {
	rawPayload, err := gatewayBytes(payload, payloadBase64, "payload")
	if err != nil {
		return store.RecordAppend{}, err
	}
	rawKey, err := gatewayBytes(key, keyBase64, "key")
	if err != nil {
		return store.RecordAppend{}, err
	}
	record := store.NewRecordAppend(rawPayload)
	record.Key = rawKey
	record.Headers, err = gatewayHeaders(headers)
	return record, err
}

func gatewayBytes(value, base64Value, field string) ([]byte, error) {
	if strings.TrimSpace(base64Value) == "" {
		return []byte(value), nil
	}
	out, err := base64.StdEncoding.DecodeString(base64Value)
	if err != nil {
		return nil, brokerStoreError(store.CodeInvalidArgument, "invalid base64 %s", field)
	}
	return out, nil
}

func gatewayHeaders(headers []gatewayHeader) ([]store.RecordHeader, error) {
	out := collectionlist.NewListWithCapacity[store.RecordHeader](len(headers))
	for index := range headers {
		value, err := gatewayBytes(headers[index].Value, headers[index].ValueBase64, "header "+headers[index].Key)
		if err != nil {
			return nil, err
		}
		out.Add(store.RecordHeader{Key: headers[index].Key, Value: value})
	}
	return out.Values(), nil
}

func gatewayPartitioning(in *gatewayProduceInput) PublishPartitioning {
	if in.Body.Partition != nil {
		return PublishPartitioning{Mode: PartitionExplicit, Partition: *in.Body.Partition}
	}
	if in.Body.RoutingKey != "" {
		return PublishPartitioning{Mode: PartitionRoutingKeyHash, RoutingKey: in.Body.RoutingKey}
	}
	if in.Body.Key != "" || in.Body.KeyBase64 != "" {
		return PublishPartitioning{Mode: PartitionKeyHash}
	}
	return PublishPartitioning{Mode: PartitionRoundRobin}
}

func gatewayFetchIsolation(value string) FetchIsolation {
	if value == string(FetchIsolationReadCommitted) {
		return FetchIsolationReadCommitted
	}
	return FetchIsolationReadUncommitted
}

func gatewayFetchOffset(value string) (uint64, bool, error) {
	if strings.TrimSpace(value) == "" {
		return 0, false, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, false, brokerStoreError(store.CodeInvalidArgument, "invalid fetch offset %q", value)
	}
	return parsed, true, nil
}

func gatewayRecords(records []store.Record) []gatewayRecordResponse {
	out := collectionlist.NewListWithCapacity[gatewayRecordResponse](len(records))
	for index := range records {
		out.Add(gatewayRecord(records[index]))
	}
	return out.Values()
}

func gatewayRecord(record store.Record) gatewayRecordResponse {
	return gatewayRecordResponse{
		Offset:        record.Offset,
		TimestampMS:   record.TimestampMS,
		RoutingKey:    recordRoutingKey(record),
		KeyBase64:     base64.StdEncoding.EncodeToString(record.Key),
		Headers:       gatewayHeadersFromStore(record.Headers),
		Tombstone:     record.IsTombstone(),
		ExpiresAtMS:   cloneUint64Ptr(record.ExpiresAtMS),
		Payload:       gatewayUTF8(record.Payload),
		PayloadBase64: base64.StdEncoding.EncodeToString(record.Payload),
		NextOffset:    record.Offset + 1,
	}
}

func gatewayHeadersFromStore(headers []store.RecordHeader) []gatewayHeader {
	out := collectionlist.NewListWithCapacity[gatewayHeader](len(headers))
	for index := range headers {
		value := headers[index].Value
		out.Add(gatewayHeader{
			Key:         headers[index].Key,
			Value:       gatewayUTF8(value),
			ValueBase64: base64.StdEncoding.EncodeToString(value),
		})
	}
	return out.Values()
}

func gatewayUTF8(value []byte) string {
	if !utf8.Valid(value) {
		return ""
	}
	return string(value)
}
