package broker_test

import (
	"bytes"
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/store"
)

func TestBrokerShardedStorxMessageRuntimePersistsByShard(t *testing.T) {
	cfg, b, stop := newShardedStorxTestBroker(t)
	defer stop()
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)
	publishPartition(ctx, t, b, 0, []byte("p0"))
	publishPartition(ctx, t, b, 1, []byte("p1"))
	assertPartitionPayload(ctx, t, b, 0, "p0")
	assertPartitionPayload(ctx, t, b, 1, "p1")
	assertTopicMessageSnapshot(t, b, 1, "p1")

	stop()
	assertShardPayload(t, cfg, 0, 0, "p0")
	assertShardPayload(t, cfg, 1, 1, "p1")
}

func TestBrokerShardedStorxLivePartitionReassignmentMovesData(t *testing.T) {
	cfg, b, stop := newShardedStorxTestBroker(t)
	defer stop()
	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)
	publishPartition(ctx, t, b, 1, []byte("p1-a"))
	publishPartition(ctx, t, b, 1, []byte("p1-b"))

	requireNoError(t, b.ReassignPartition(ctx, "orders", 1, 0))
	publishPartition(ctx, t, b, 1, []byte("p1-c"))
	assertPartitionPayloads(ctx, t, b, 1, "p1-a", "p1-b", "p1-c")

	stop()
	assertShardPayloads(t, cfg, 0, 1, "p1-a", "p1-b", "p1-c")
	assertShardEmpty(t, cfg, 1, 1)
}

func TestBrokerShardedStorxMaintenanceRunsAcrossShards(t *testing.T) {
	_, b, stop := newShardedStorxTestBroker(t)
	defer stop()
	ctx := context.Background()
	retentionMS := uint64(1)
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	topic.RetentionMS = &retentionMS
	createTopic(ctx, t, b, topic)

	oldMS := uint64(1)
	publishPartitionRecord(ctx, t, b, 0, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("p0")})
	publishPartitionRecord(ctx, t, b, 1, store.RecordAppend{TimestampMS: &oldMS, Payload: []byte("p1")})
	result, err := b.EnforceRetentionOnce(ctx)
	requireNoError(t, err)
	if result.RemovedRecords != 2 {
		t.Fatalf("unexpected retention result: %#v", result)
	}
	assertPartitionEmpty(ctx, t, b, 0)
	assertPartitionEmpty(ctx, t, b, 1)
}

func newShardedStorxTestBroker(t *testing.T) (broker.Config, *broker.Broker, func()) {
	t.Helper()
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Broker.DataShardCount = 2
	centralLog := openBrokerStorxLog(t, cfg.SegmentLogPath())
	metaStore := store.NewMemoryStore()
	b, err := broker.NewWithStores(cfg, centralLog, metaStore)
	requireNoError(t, err)
	stopped := false
	closed := false
	return cfg, b, func() {
		if closed {
			return
		}
		if !stopped {
			requireNoError(t, b.Stop(context.Background()))
			stopped = true
		}
		closeBrokerStorxLog(t, centralLog)
		closed = true
	}
}

func publishPartition(ctx context.Context, t *testing.T, b *broker.Broker, partition uint32, payload []byte) {
	t.Helper()
	result := publishPartitionRecord(ctx, t, b, partition, store.RecordAppend{Payload: payload})
	if !bytes.Equal(result.Record.Payload, payload) {
		t.Fatalf("unexpected publish result: %#v", result)
	}
}

func publishPartitionRecord(ctx context.Context, t *testing.T, b *broker.Broker, partition uint32, record store.RecordAppend) broker.ProduceResult {
	t.Helper()
	result, err := b.PublishRecord(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: partition}, record)
	requireNoError(t, err)
	if result.Partition != partition {
		t.Fatalf("unexpected publish result: %#v", result)
	}
	return result
}

func assertPartitionPayload(ctx context.Context, t *testing.T, b *broker.Broker, partition uint32, want string) {
	t.Helper()
	assertPartitionPayloads(ctx, t, b, partition, want)
}

func assertPartitionPayloads(ctx context.Context, t *testing.T, b *broker.Broker, partition uint32, want ...string) {
	t.Helper()
	offset := uint64(0)
	poll, err := b.Fetch(ctx, "c1", "orders", partition, &offset, 10)
	requireNoError(t, err)
	requireShardedRecordPayloads(t, poll.Records, want...)
}

func assertPartitionEmpty(ctx context.Context, t *testing.T, b *broker.Broker, partition uint32) {
	t.Helper()
	poll, err := b.Fetch(ctx, "c1", "orders", partition, nil, 10)
	requireNoError(t, err)
	if len(poll.Records) != 0 {
		t.Fatalf("expected empty partition %d poll result, got: %#v", partition, poll)
	}
}

func assertTopicMessageSnapshot(t *testing.T, b *broker.Broker, partition uint32, want string) {
	t.Helper()
	page, err := b.TopicMessagesSnapshot("orders", partition, 0, 10)
	requireNoError(t, err)
	if len(page.Records) != 1 || page.Records[0].PayloadUTF8Preview != want {
		t.Fatalf("unexpected topic message snapshot: %#v", page)
	}
}

func assertShardPayload(t *testing.T, cfg broker.Config, shardID store.ShardID, partition uint32, want string) {
	t.Helper()
	assertShardPayloads(t, cfg, shardID, partition, want)
}

func assertShardPayloads(t *testing.T, cfg broker.Config, shardID store.ShardID, partition uint32, want ...string) {
	t.Helper()
	logStore := openBrokerStorxLog(t, cfg.ShardSegmentLogPath(shardID))
	defer closeBrokerStorxLog(t, logStore)
	records, err := logStore.ReadFrom(store.NewTopicPartition("orders", partition), 0, 10)
	requireNoError(t, err)
	requireShardedRecordPayloads(t, records, want...)
}

func assertShardEmpty(t *testing.T, cfg broker.Config, shardID store.ShardID, partition uint32) {
	t.Helper()
	logStore := openBrokerStorxLog(t, cfg.ShardSegmentLogPath(shardID))
	defer closeBrokerStorxLog(t, logStore)
	records, err := logStore.ReadFrom(store.NewTopicPartition("orders", partition), 0, 10)
	requireNoError(t, err)
	if len(records) != 0 {
		t.Fatalf("expected empty shard %d partition %d, got: %#v", shardID, partition, records)
	}
}

func requireShardedRecordPayloads(t *testing.T, records []store.Record, want ...string) {
	t.Helper()
	if len(records) != len(want) {
		t.Fatalf("unexpected records: %#v", records)
	}
	for i, payload := range want {
		if string(records[i].Payload) != payload {
			t.Fatalf("unexpected record %d payload: got %q want %q in %#v", i, records[i].Payload, payload, records)
		}
		if records[i].Offset != uint64(i) {
			t.Fatalf("unexpected record %d offset: got %d want %d in %#v", i, records[i].Offset, i, records)
		}
	}
}

func openBrokerStorxLog(t *testing.T, path string) *store.StorxLogStore {
	t.Helper()
	logStore, err := store.OpenStorxLogStore(path)
	requireNoError(t, err)
	return logStore
}

func closeBrokerStorxLog(t *testing.T, logStore *store.StorxLogStore) {
	t.Helper()
	if err := logStore.Close(); err != nil {
		t.Logf("close storx log: %v", err)
	}
}
