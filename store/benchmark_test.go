package store_test

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/DaiYuANg/ech0/store"
)

var storeBenchmarkRecordSink store.Record
var storeBenchmarkRecordsSink []store.Record

func BenchmarkMemoryStoreAppend1KB(b *testing.B) {
	st := store.NewMemoryStore()
	mustBenchmarkNoError(b, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	payload := benchmarkStorePayload(1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		record, err := st.Append(tp, payload)
		if err != nil {
			b.Fatal(err)
		}
		storeBenchmarkRecordSink = record
	}
}

func BenchmarkStorxLogStoreAppend1KB(b *testing.B) {
	st := openBenchmarkLogStore(b)
	defer closeBenchmarkLogStore(b, st)
	mustBenchmarkNoError(b, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	payload := benchmarkStorePayload(1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		record, err := st.Append(tp, payload)
		if err != nil {
			b.Fatal(err)
		}
		storeBenchmarkRecordSink = record
	}
}

func BenchmarkStorxLogStoreAppendBatch100x1KB(b *testing.B) {
	st := openBenchmarkLogStore(b)
	defer closeBenchmarkLogStore(b, st)
	mustBenchmarkNoError(b, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	records := benchmarkAppendRecords(100, 1024)
	b.ReportAllocs()
	b.SetBytes(int64(100 * 1024))
	for b.Loop() {
		appended, err := st.AppendRecordsBatch(tp, records)
		if err != nil {
			b.Fatal(err)
		}
		storeBenchmarkRecordsSink = appended
	}
}

func BenchmarkStorxLogStoreReadFrom100x1KB(b *testing.B) {
	st := openBenchmarkLogStore(b)
	defer closeBenchmarkLogStore(b, st)
	mustBenchmarkNoError(b, st.CreateTopic(store.NewTopicConfig("orders")))
	tp := store.NewTopicPartition("orders", 0)
	records := benchmarkAppendRecords(4096, 1024)
	_, err := st.AppendRecordsBatch(tp, records)
	mustBenchmarkNoError(b, err)
	b.ReportAllocs()
	b.SetBytes(int64(100 * 1024))
	for i := 0; b.Loop(); i++ {
		offset := uint64((i % 32) * 100)
		read, err := st.ReadFrom(tp, offset, 100)
		if err != nil {
			b.Fatal(err)
		}
		storeBenchmarkRecordsSink = read
	}
}

func openBenchmarkLogStore(b *testing.B) *store.StorxLogStore {
	b.Helper()
	st, err := store.OpenStorxLogStore(filepath.Join(b.TempDir(), "segments"))
	mustBenchmarkNoError(b, err)
	return st
}

func closeBenchmarkLogStore(b *testing.B, st *store.StorxLogStore) {
	b.Helper()
	if err := st.Close(); err != nil {
		b.Logf("close log store: %v", err)
	}
}

func benchmarkAppendRecords(count, payloadBytes int) []store.RecordAppend {
	records := make([]store.RecordAppend, 0, count)
	payload := benchmarkStorePayload(payloadBytes)
	for i := range count {
		records = append(records, store.RecordAppend{
			Key:     []byte("key-" + strconv.Itoa(i%128)),
			Headers: []store.RecordHeader{{Key: "trace_id", Value: []byte("trace-" + strconv.Itoa(i))}},
			Payload: payload,
		})
	}
	return records
}

func benchmarkStorePayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	return payload
}

func mustBenchmarkNoError(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
