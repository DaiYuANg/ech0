package broker_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/store"
	"github.com/arcgolabs/mapper"
)

var (
	mapperBenchmarkStoreHeadersSink    []store.RecordHeader
	mapperBenchmarkProtocolHeadersSink []protocol.MessageHeader
	mapperBenchmarkFetchRecordsSink    []protocol.FetchRecord
)

func BenchmarkMapperProtocolHeadersToStoreManual64(b *testing.B) {
	headers := benchmarkProtocolHeaders(64, 32)
	b.ReportAllocs()
	for b.Loop() {
		mapperBenchmarkStoreHeadersSink = manualStoreHeadersFromProtocol(headers)
	}
}

func BenchmarkMapperProtocolHeadersToStoreMapper64(b *testing.B) {
	m := newBenchmarkMapper()
	headers := benchmarkProtocolHeaders(64, 32)
	out, err := mapProtocolHeadersWithMapper(m, headers)
	mustMapperBenchmarkNoError(b, err)
	mapperBenchmarkStoreHeadersSink = out

	b.ReportAllocs()
	for b.Loop() {
		out, err := mapProtocolHeadersWithMapper(m, headers)
		if err != nil {
			b.Fatal(err)
		}
		mapperBenchmarkStoreHeadersSink = out
	}
}

func BenchmarkMapperStoreHeadersToProtocolManual64(b *testing.B) {
	headers := benchmarkStoreHeaders(64, 32)
	b.ReportAllocs()
	for b.Loop() {
		mapperBenchmarkProtocolHeadersSink = manualProtocolHeadersFromStore(headers)
	}
}

func BenchmarkMapperStoreHeadersToProtocolMapper64(b *testing.B) {
	m := newBenchmarkMapper()
	headers := benchmarkStoreHeaders(64, 32)
	out, err := mapStoreHeadersWithMapper(m, headers)
	mustMapperBenchmarkNoError(b, err)
	mapperBenchmarkProtocolHeadersSink = out

	b.ReportAllocs()
	for b.Loop() {
		out, err := mapStoreHeadersWithMapper(m, headers)
		if err != nil {
			b.Fatal(err)
		}
		mapperBenchmarkProtocolHeadersSink = out
	}
}

func BenchmarkMapperFetchRecordsFromStoreManual100x1KB(b *testing.B) {
	records := benchmarkStoreRecords(100, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(records) * 1024))
	for b.Loop() {
		mapperBenchmarkFetchRecordsSink = manualFetchRecordsFromStore(records)
	}
}

func BenchmarkMapperFetchRecordsFromStoreMapper100x1KB(b *testing.B) {
	m := newBenchmarkMapper()
	records := benchmarkStoreRecords(100, 1024)
	out, err := mapFetchRecordsWithMapper(m, records)
	mustMapperBenchmarkNoError(b, err)
	mapperBenchmarkFetchRecordsSink = out

	b.ReportAllocs()
	b.SetBytes(int64(len(records) * 1024))
	for b.Loop() {
		out, err := mapFetchRecordsWithMapper(m, records)
		if err != nil {
			b.Fatal(err)
		}
		mapperBenchmarkFetchRecordsSink = out
	}
}

func newBenchmarkMapper() *mapper.Mapper {
	return mapper.New(
		mapper.Converter(func(value []byte) []byte {
			return append([]byte(nil), value...)
		}),
	)
}

func mapProtocolHeadersWithMapper(m *mapper.Mapper, headers []protocol.MessageHeader) ([]store.RecordHeader, error) {
	var out []store.RecordHeader
	if err := m.MapInto(&out, headers); err != nil {
		return nil, fmt.Errorf("map protocol headers to store headers: %w", err)
	}
	return out, nil
}

func mapStoreHeadersWithMapper(m *mapper.Mapper, headers []store.RecordHeader) ([]protocol.MessageHeader, error) {
	var out []protocol.MessageHeader
	if err := m.MapInto(&out, headers); err != nil {
		return nil, fmt.Errorf("map store headers to protocol headers: %w", err)
	}
	return out, nil
}

func mapFetchRecordsWithMapper(m *mapper.Mapper, records []store.Record) ([]protocol.FetchRecord, error) {
	var out []protocol.FetchRecord
	if err := m.MapInto(&out, records); err != nil {
		return nil, fmt.Errorf("map store records to protocol fetch records: %w", err)
	}
	for i, record := range records {
		out[i].Tombstone = record.IsTombstone()
	}
	return out, nil
}

func manualStoreHeadersFromProtocol(headers []protocol.MessageHeader) []store.RecordHeader {
	out := make([]store.RecordHeader, 0, len(headers))
	for _, header := range headers {
		out = append(out, store.RecordHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}
	return out
}

func manualProtocolHeadersFromStore(headers []store.RecordHeader) []protocol.MessageHeader {
	out := make([]protocol.MessageHeader, 0, len(headers))
	for _, header := range headers {
		out = append(out, protocol.MessageHeader{
			Key:   header.Key,
			Value: append([]byte(nil), header.Value...),
		})
	}
	return out
}

func manualFetchRecordsFromStore(records []store.Record) []protocol.FetchRecord {
	out := make([]protocol.FetchRecord, 0, len(records))
	for _, record := range records {
		out = append(out, protocol.FetchRecord{
			Offset:      record.Offset,
			TimestampMS: record.TimestampMS,
			Key:         append([]byte(nil), record.Key...),
			Headers:     manualProtocolHeadersFromStore(record.Headers),
			Tombstone:   record.IsTombstone(),
			Payload:     append([]byte(nil), record.Payload...),
		})
	}
	return out
}

func benchmarkProtocolHeaders(count, valueBytes int) []protocol.MessageHeader {
	headers := make([]protocol.MessageHeader, 0, count)
	for i := range count {
		headers = append(headers, protocol.MessageHeader{
			Key:   "header-" + strconv.Itoa(i),
			Value: benchmarkMapperPayload(valueBytes),
		})
	}
	return headers
}

func benchmarkStoreHeaders(count, valueBytes int) []store.RecordHeader {
	headers := make([]store.RecordHeader, 0, count)
	for i := range count {
		headers = append(headers, store.RecordHeader{
			Key:   "header-" + strconv.Itoa(i),
			Value: benchmarkMapperPayload(valueBytes),
		})
	}
	return headers
}

func benchmarkStoreRecords(count, payloadBytes int) []store.Record {
	records := make([]store.Record, 0, count)
	for i := range count {
		records = append(records, store.Record{
			Offset:      uint64(i),
			TimestampMS: 1700000000000 + uint64(i),
			Key:         []byte("key-" + strconv.Itoa(i)),
			Headers:     benchmarkStoreHeaders(4, 32),
			Attributes:  benchmarkRecordAttributes(i),
			Payload:     benchmarkMapperPayload(payloadBytes),
		})
	}
	return records
}

func benchmarkRecordAttributes(index int) uint16 {
	if index%10 == 0 {
		return store.RecordAttributeTombstone
	}
	return 0
}

func benchmarkMapperPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}

func mustMapperBenchmarkNoError(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
