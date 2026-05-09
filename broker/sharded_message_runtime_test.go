package broker_test

import (
	"bytes"
	"context"
	"testing"

	broker "github.com/DaiYuANg/ech0/broker"
	"github.com/DaiYuANg/ech0/store"
)

func TestBrokerShardedStorxMessageRuntimePersistsByShard(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Broker.DataDir = t.TempDir()
	cfg.Broker.DataShardCount = 2

	centralLog := openBrokerStorxLog(t, cfg.SegmentLogPath(), "")
	defer closeBrokerStorxLog(t, centralLog)
	metaStore, err := store.OpenStorxMetadataStore(cfg.MetadataPath())
	requireNoError(t, err)
	defer closeBrokerStorxMetadata(t, metaStore)

	b, err := broker.NewWithStores(cfg, centralLog, metaStore)
	requireNoError(t, err)
	stopped := false
	defer func() {
		if !stopped {
			requireNoError(t, b.Stop(context.Background()))
		}
	}()

	ctx := context.Background()
	topic := store.NewTopicConfig("orders")
	topic.Partitions = 2
	createTopic(ctx, t, b, topic)
	publishPartition(ctx, t, b, 0, []byte("p0"))
	publishPartition(ctx, t, b, 1, []byte("p1"))
	assertPartitionPayload(ctx, t, b, 0, "p0")
	assertPartitionPayload(ctx, t, b, 1, "p1")
	assertTopicMessageSnapshot(t, b, 1, "p1")

	requireNoError(t, b.Stop(ctx))
	stopped = true
	assertShardPayload(t, cfg, 0, 0, "p0")
	assertShardPayload(t, cfg, 1, 1, "p1")
}

func publishPartition(ctx context.Context, t *testing.T, b *broker.Broker, partition uint32, payload []byte) {
	t.Helper()
	result, err := b.Publish(ctx, "orders", broker.PublishPartitioning{Mode: broker.PartitionExplicit, Partition: partition}, nil, false, payload)
	requireNoError(t, err)
	if result.Partition != partition || !bytes.Equal(result.Record.Payload, payload) {
		t.Fatalf("unexpected publish result: %#v", result)
	}
}

func assertPartitionPayload(ctx context.Context, t *testing.T, b *broker.Broker, partition uint32, want string) {
	t.Helper()
	poll, err := b.Fetch(ctx, "c1", "orders", partition, nil, 10)
	requireNoError(t, err)
	if len(poll.Records) != 1 || string(poll.Records[0].Payload) != want {
		t.Fatalf("unexpected partition %d poll result: %#v", partition, poll)
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
	logStore := openBrokerStorxLog(t, cfg.ShardSegmentLogPath(shardID), cfg.ShardBadgerPath(shardID))
	defer closeBrokerStorxLog(t, logStore)
	records, err := logStore.ReadFrom(store.NewTopicPartition("orders", partition), 0, 10)
	requireNoError(t, err)
	if len(records) != 1 || string(records[0].Payload) != want {
		t.Fatalf("unexpected shard %d records: %#v", shardID, records)
	}
}

func openBrokerStorxLog(t *testing.T, path, indexPath string) *store.StorxLogStore {
	t.Helper()
	logStore, err := store.OpenStorxLogStoreWithOptions(path, store.StorxLogOptions{IndexPath: indexPath})
	requireNoError(t, err)
	return logStore
}

func closeBrokerStorxLog(t *testing.T, logStore *store.StorxLogStore) {
	t.Helper()
	if err := logStore.Close(); err != nil {
		t.Logf("close storx log: %v", err)
	}
}

func closeBrokerStorxMetadata(t *testing.T, metaStore *store.StorxMetadataStore) {
	t.Helper()
	if err := metaStore.Close(); err != nil {
		t.Logf("close storx metadata: %v", err)
	}
}
