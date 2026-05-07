package store

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/arcgolabs/storx/bboltx"
	"github.com/arcgolabs/storx/codec"
	"github.com/arcgolabs/storx/keycodec"
)

const (
	bucketLogTopics  = "log_topics"
	bucketLogRecords = "log_records"
	bucketLogOffsets = "log_next_offsets"
)

type StorxLogStore struct {
	mu          sync.Mutex
	db          *bboltx.DB
	topics      *bboltx.Bucket[string, TopicConfig]
	records     *bboltx.Bucket[string, Record]
	nextOffsets *bboltx.Bucket[string, uint64]
}

func OpenStorxLogStore(path string) (*StorxLogStore, error) {
	if path == "" {
		return nil, E(CodeInvalidArgument, "segment log path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, err
	}
	db, err := bboltx.Open(path, 0o600, nil)
	if err != nil {
		return nil, err
	}
	keyCodec := keycodec.String()
	return &StorxLogStore{
		db:          db,
		topics:      bboltx.NewBucketWithDB(db, bucketLogTopics, keyCodec, codec.JSON[TopicConfig]()),
		records:     bboltx.NewBucketWithDB(db, bucketLogRecords, keyCodec, codec.JSON[Record]()),
		nextOffsets: bboltx.NewBucketWithDB(db, bucketLogOffsets, keyCodec, codec.JSON[uint64]()),
	}, nil
}

func (s *StorxLogStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *StorxLogStore) CreateTopic(topic TopicConfig) error {
	if topic.Name == "" {
		return E(CodeInvalidArgument, "topic name is required")
	}
	if topic.Partitions == 0 {
		return E(CodeInvalidArgument, "topic %s must have at least one partition", topic.Name)
	}
	normalizeTopic(&topic)

	s.mu.Lock()
	defer s.mu.Unlock()
	exists, err := s.topics.Exists(context.Background(), topic.Name)
	if err != nil {
		return err
	}
	if exists {
		return E(CodeTopicExists, "topic %s already exists", topic.Name)
	}
	if err := s.topics.Put(context.Background(), topic.Name, cloneTopic(topic)); err != nil {
		return err
	}
	for partition := uint32(0); partition < topic.Partitions; partition++ {
		if err := s.nextOffsets.Put(context.Background(), nextOffsetKey(NewTopicPartition(topic.Name, partition)), 0); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorxLogStore) TopicExists(topic string) (bool, error) {
	return s.topics.Exists(context.Background(), topic)
}

func (s *StorxLogStore) Append(topicPartition TopicPartition, payload []byte) (Record, error) {
	return s.AppendRecord(topicPartition, NewRecordAppend(payload))
}

func (s *StorxLogStore) AppendRecord(topicPartition TopicPartition, appendRecord RecordAppend) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, err := s.loadTopicForPartition(topicPartition)
	if err != nil {
		return Record{}, err
	}
	if uint32(len(appendRecord.Payload)) > topic.MaxMessageBytes {
		return Record{}, E(CodeInvalidArgument, "payload size %d exceeds max_message_bytes %d", len(appendRecord.Payload), topic.MaxMessageBytes)
	}
	offsetKey := nextOffsetKey(topicPartition)
	offset, ok, err := s.nextOffsets.Get(context.Background(), offsetKey)
	if err != nil {
		return Record{}, err
	}
	if !ok {
		offset = 0
	}
	timestamp := NowMS()
	if appendRecord.TimestampMS != nil {
		timestamp = *appendRecord.TimestampMS
	}
	record := Record{
		Offset:      offset,
		TimestampMS: timestamp,
		Key:         cloneBytes(appendRecord.Key),
		Headers:     cloneHeaders(appendRecord.Headers),
		Attributes:  appendRecord.Attributes,
		Payload:     cloneBytes(appendRecord.Payload),
	}
	if err := s.records.Put(context.Background(), recordKey(topicPartition, offset), record); err != nil {
		return Record{}, err
	}
	if err := s.nextOffsets.Put(context.Background(), offsetKey, offset+1); err != nil {
		return Record{}, err
	}
	return cloneRecord(record), nil
}

func (s *StorxLogStore) AppendRecordsBatch(topicPartition TopicPartition, records []RecordAppend) ([]Record, error) {
	out := make([]Record, 0, len(records))
	for _, record := range records {
		appended, err := s.AppendRecord(topicPartition, record)
		if err != nil {
			return nil, err
		}
		out = append(out, appended)
	}
	return out, nil
}

func (s *StorxLogStore) ReadFrom(topicPartition TopicPartition, offset uint64, maxRecords int) ([]Record, error) {
	if maxRecords <= 0 {
		return nil, nil
	}
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return nil, err
	}
	entries, err := s.records.List(
		context.Background(),
		bboltx.WithPrefix[string]([]byte(recordPrefix(topicPartition))),
		bboltx.WithStart(recordKey(topicPartition, offset)),
		bboltx.WithLimit[string](maxRecords),
	)
	if err != nil {
		return nil, err
	}
	out := make([]Record, 0, len(entries))
	for _, entry := range entries {
		out = append(out, cloneRecord(entry.Value))
	}
	return out, nil
}

func (s *StorxLogStore) LastOffset(topicPartition TopicPartition) (*uint64, error) {
	if _, err := s.loadTopicForPartition(topicPartition); err != nil {
		return nil, err
	}
	next, ok, err := s.nextOffsets.Get(context.Background(), nextOffsetKey(topicPartition))
	if err != nil || !ok || next == 0 {
		return nil, err
	}
	last := next - 1
	return &last, nil
}

func (s *StorxLogStore) Snapshot() (Snapshot, error) {
	topics := make([]TopicConfig, 0)
	if err := s.topics.Walk(context.Background(), func(entry bboltx.Entry[string, TopicConfig]) error {
		topics = append(topics, cloneTopic(entry.Value))
		return nil
	}); err != nil {
		return Snapshot{}, err
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })

	records := make(map[string][]Record)
	if err := s.records.Walk(context.Background(), func(entry bboltx.Entry[string, Record]) error {
		tp, err := parseRecordKey(entry.Key)
		if err != nil {
			return err
		}
		key := partitionKey(tp)
		records[key] = append(records[key], cloneRecord(entry.Value))
		return nil
	}); err != nil {
		return Snapshot{}, err
	}
	for key := range records {
		sort.Slice(records[key], func(i, j int) bool { return records[key][i].Offset < records[key][j].Offset })
	}
	return Snapshot{Topics: topics, Records: records}, nil
}

func (s *StorxLogStore) Restore(snapshot Snapshot) error {
	for _, bucket := range []interface{ Clear(context.Context) error }{
		bucketClearer[string, TopicConfig]{s.topics},
		bucketClearer[string, Record]{s.records},
		bucketClearer[string, uint64]{s.nextOffsets},
	} {
		if err := bucket.Clear(context.Background()); err != nil {
			return err
		}
	}
	for _, topic := range snapshot.Topics {
		if err := s.CreateTopic(topic); err != nil {
			return err
		}
	}
	nextOffsets := make(map[TopicPartition]uint64)
	for key, topicRecords := range snapshot.Records {
		tp, err := parsePartitionKey(key)
		if err != nil {
			return err
		}
		for _, record := range topicRecords {
			if err := s.records.Put(context.Background(), recordKey(tp, record.Offset), cloneRecord(record)); err != nil {
				return err
			}
			if record.Offset+1 > nextOffsets[tp] {
				nextOffsets[tp] = record.Offset + 1
			}
		}
	}
	for tp, next := range nextOffsets {
		if err := s.nextOffsets.Put(context.Background(), nextOffsetKey(tp), next); err != nil {
			return err
		}
	}
	return nil
}

func (s *StorxLogStore) loadTopicForPartition(topicPartition TopicPartition) (TopicConfig, error) {
	topic, ok, err := s.topics.Get(context.Background(), topicPartition.Topic)
	if err != nil {
		return TopicConfig{}, err
	}
	if !ok {
		return TopicConfig{}, E(CodeTopicNotFound, "topic %s not found", topicPartition.Topic)
	}
	if topicPartition.Partition >= topic.Partitions {
		return TopicConfig{}, E(CodePartitionNotFound, "partition %s/%d not found", topicPartition.Topic, topicPartition.Partition)
	}
	return cloneTopic(topic), nil
}

func recordPrefix(tp TopicPartition) string {
	return tp.Topic + "\x00" + strconv.FormatUint(uint64(tp.Partition), 10) + "\x00"
}

func recordKey(tp TopicPartition, offset uint64) string {
	return recordPrefix(tp) + fmtOffset(offset)
}

func parseRecordKey(key string) (TopicPartition, error) {
	parts := strings.Split(key, "\x00")
	if len(parts) != 3 {
		return TopicPartition{}, E(CodeCodec, "invalid record key %q", key)
	}
	partition, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return TopicPartition{}, E(CodeCodec, "invalid partition in record key %q: %v", key, err)
	}
	return NewTopicPartition(parts[0], uint32(partition)), nil
}

func nextOffsetKey(tp TopicPartition) string {
	return tp.Topic + "\x00" + strconv.FormatUint(uint64(tp.Partition), 10)
}

func fmtOffset(offset uint64) string {
	raw := strconv.FormatUint(offset, 10)
	if len(raw) >= 20 {
		return raw
	}
	return strings.Repeat("0", 20-len(raw)) + raw
}
