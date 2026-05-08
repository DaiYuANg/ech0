package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/storx/badgerx"
	"github.com/arcgolabs/storx/codec"
	"github.com/arcgolabs/storx/keycodec"
	"github.com/dgraph-io/badger/v4"
)

const (
	indexLogTopics  = "message/topics"
	indexLogRecords = "message/records"
	indexLogOffsets = "message/next_offsets"
)

type StorxLogStore struct {
	mu               sync.Mutex
	partitionLocksMu sync.Mutex
	writersMu        sync.Mutex
	readersMu        sync.Mutex
	rootDir          string
	segmentsDir      string
	compression      segmentFrameCompression
	index            *badgerx.DB
	topics           *badgerx.Namespace[string, TopicConfig]
	records          *badgerx.Namespace[recordIndexKey, segmentRecordPointer]
	nextOffsets      *badgerx.Namespace[partitionIndexKey, uint64]
	metrics          StoreMetrics
	partitionLocks   *collectionmapping.Map[TopicPartition, *sync.Mutex]
	writers          *collectionmapping.Map[string, *segmentWriter]
	readers          *collectionmapping.Map[string, *segmentReader]
}

type segmentRecordPointer struct {
	Topic       string `json:"topic"`
	Partition   uint32 `json:"partition"`
	Offset      uint64 `json:"offset"`
	SegmentID   uint64 `json:"segment_id"`
	Position    int64  `json:"position"`
	Length      int    `json:"length"`
	TimestampMS uint64 `json:"timestamp_ms"`
	Attributes  uint16 `json:"attributes"`
}

type appendRecordPlan struct {
	topic  TopicConfig
	offset uint64
	record Record
	frame  []byte
}

func OpenStorxLogStore(path string) (*StorxLogStore, error) {
	return OpenStorxLogStoreWithOptions(path, StorxLogOptions{})
}

func OpenStorxLogStoreWithOptions(path string, options StorxLogOptions) (*StorxLogStore, error) {
	options, optionsErr := options.normalize()
	if optionsErr != nil {
		return nil, optionsErr
	}
	compression, compressionErr := newSegmentFrameCompression(options.Compression)
	if compressionErr != nil {
		return nil, compressionErr
	}
	rootDir, normalizeErr := normalizeSegmentRoot(path)
	if normalizeErr != nil {
		return nil, errors.Join(normalizeErr, compression.close())
	}
	segmentsDir := filepath.Join(rootDir, "segments")
	if mkdirErr := os.MkdirAll(segmentsDir, 0o750); mkdirErr != nil {
		return nil, errors.Join(wrapExternal(mkdirErr, "create segment log directory"), compression.close())
	}
	index, err := openSegmentIndex(filepath.Join(rootDir, "index.badger"), options)
	if err != nil {
		return nil, errors.Join(err, compression.close())
	}
	stringKeyCodec := keycodec.String()
	return &StorxLogStore{
		rootDir:        rootDir,
		segmentsDir:    segmentsDir,
		compression:    compression,
		index:          index,
		topics:         badgerx.NewNamespaceWithDB(index, indexLogTopics, stringKeyCodec, codec.JSON[TopicConfig]()),
		records:        badgerx.NewNamespaceWithDB(index, indexLogRecords, recordIndexKeyCodec(), codec.JSON[segmentRecordPointer]()),
		nextOffsets:    badgerx.NewNamespaceWithDB(index, indexLogOffsets, partitionIndexKeyCodec(), codec.JSON[uint64]()),
		metrics:        options.Metrics,
		partitionLocks: collectionmapping.NewMap[TopicPartition, *sync.Mutex](),
		writers:        collectionmapping.NewMap[string, *segmentWriter](),
		readers:        collectionmapping.NewMap[string, *segmentReader](),
	}, nil
}

func normalizeSegmentRoot(path string) (string, error) {
	if path == "" {
		return "", E(CodeInvalidArgument, "segment log path is required")
	}
	root := path
	if filepath.Ext(path) != "" {
		root = filepath.Dir(path)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return "", wrapExternal(err, "create segment log root")
	}
	return root, nil
}

func openSegmentIndex(path string, storeOptions StorxLogOptions) (*badgerx.DB, error) {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return nil, wrapExternal(err, "create segment index directory")
	}
	options := badger.DefaultOptions(path).WithLogger(nil)
	index, err := badgerx.Open(
		options,
		badgerx.WithDBLogger(storeOptions.Logger),
		badgerx.WithDBObservers(storeOptions.Observers...),
	)
	if err != nil {
		return nil, wrapExternal(err, "open segment index")
	}
	return index, nil
}

func (s *StorxLogStore) Close() error {
	if s == nil {
		return nil
	}
	return errors.Join(
		s.closeSegmentWriters(),
		s.closeSegmentReaders(),
		s.compression.close(),
		closeSegmentIndex(s.index),
	)
}

func closeSegmentIndex(index *badgerx.DB) error {
	if index == nil {
		return nil
	}
	return wrapExternal(index.Close(), "close segment index")
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
		return wrapExternal(err, "check log topic")
	}
	if exists {
		return E(CodeTopicExists, "topic %s already exists", topic.Name)
	}
	if err := s.topics.Set(context.Background(), topic.Name, cloneTopic(topic)); err != nil {
		return wrapExternal(err, "save log topic")
	}
	for partition := range topic.Partitions {
		tp := NewTopicPartition(topic.Name, partition)
		if err := s.nextOffsets.Set(context.Background(), nextOffsetKey(tp), 0); err != nil {
			return wrapExternal(err, "initialize log partition offset")
		}
		if err := os.MkdirAll(s.partitionDir(tp), 0o750); err != nil {
			return wrapExternal(err, "create segment partition directory")
		}
	}
	return nil
}

func (s *StorxLogStore) TopicExists(topic string) (bool, error) {
	exists, err := s.topics.Exists(context.Background(), topic)
	if err != nil {
		return false, wrapExternal(err, "check log topic")
	}
	return exists, nil
}

func (s *StorxLogStore) Append(topicPartition TopicPartition, payload []byte) (Record, error) {
	return s.AppendRecord(topicPartition, NewRecordAppend(payload))
}

func (s *StorxLogStore) AppendRecord(topicPartition TopicPartition, appendRecord RecordAppend) (out Record, err error) {
	const operation = "append_record"
	totalStart := time.Now()
	defer func() {
		s.recordAppendStage(operation, "total", 1, totalStart, err)
	}()

	lockStart := time.Now()
	lock := s.partitionLock(topicPartition)
	lock.Lock()
	s.recordAppendStage(operation, "lock_wait", 1, lockStart, nil)
	defer lock.Unlock()

	plan, err := s.prepareAppendRecord(operation, topicPartition, appendRecord)
	if err != nil {
		return Record{}, err
	}
	return s.commitAppendRecord(operation, topicPartition, plan)
}

func (s *StorxLogStore) partitionLock(topicPartition TopicPartition) *sync.Mutex {
	s.partitionLocksMu.Lock()
	defer s.partitionLocksMu.Unlock()
	lock, ok := s.partitionLocks.Get(topicPartition)
	if ok {
		return lock
	}
	lock = &sync.Mutex{}
	s.partitionLocks.Set(topicPartition, lock)
	return lock
}

func (s *StorxLogStore) prepareAppendRecord(operation string, topicPartition TopicPartition, appendRecord RecordAppend) (appendRecordPlan, error) {
	topicStart := time.Now()
	topic, loadErr := s.loadTopicForPartition(topicPartition)
	s.recordAppendStage(operation, "load_topic", 1, topicStart, loadErr)
	if loadErr != nil {
		return appendRecordPlan{}, loadErr
	}
	if len(appendRecord.Payload) > int(topic.MaxMessageBytes) {
		return appendRecordPlan{}, E(CodeInvalidArgument, "payload size %d exceeds max_message_bytes %d", len(appendRecord.Payload), topic.MaxMessageBytes)
	}
	offsetStart := time.Now()
	offset, offsetErr := s.nextOffset(topicPartition)
	s.recordAppendStage(operation, "next_offset", 1, offsetStart, offsetErr)
	if offsetErr != nil {
		return appendRecordPlan{}, offsetErr
	}
	record := newStoredRecord(offset, appendRecord)
	encodeStart := time.Now()
	frame, encodeErr := encodeSegmentFrameWithCompression(record, s.compression)
	s.recordAppendStage(operation, "encode_frame", 1, encodeStart, encodeErr)
	if encodeErr != nil {
		return appendRecordPlan{}, encodeErr
	}
	return appendRecordPlan{topic: topic, offset: offset, record: record, frame: frame}, nil
}

func (s *StorxLogStore) commitAppendRecord(operation string, topicPartition TopicPartition, plan appendRecordPlan) (Record, error) {
	appendStart := time.Now()
	pointer, appendErr := s.appendFrame(plan.topic, topicPartition, plan.offset, plan.frame, plan.record)
	s.recordAppendStage(operation, "append_frame", 1, appendStart, appendErr)
	if appendErr != nil {
		return Record{}, appendErr
	}
	indexStart := time.Now()
	indexErr := s.records.Set(context.Background(), recordKey(topicPartition, plan.offset), pointer)
	s.recordAppendStage(operation, "index_set", 1, indexStart, indexErr)
	if indexErr != nil {
		return Record{}, wrapExternal(indexErr, "save segment record index")
	}
	nextOffsetStart := time.Now()
	nextOffsetErr := s.nextOffsets.Set(context.Background(), nextOffsetKey(topicPartition), plan.offset+1)
	s.recordAppendStage(operation, "next_offset_set", 1, nextOffsetStart, nextOffsetErr)
	if nextOffsetErr != nil {
		return Record{}, wrapExternal(nextOffsetErr, "advance next log offset")
	}
	return cloneRecord(plan.record), nil
}

func newStoredRecord(offset uint64, appendRecord RecordAppend) Record {
	timestamp := NowMS()
	if appendRecord.TimestampMS != nil {
		timestamp = *appendRecord.TimestampMS
	}
	return Record{
		Offset:      offset,
		TimestampMS: timestamp,
		Key:         cloneBytes(appendRecord.Key),
		Headers:     cloneHeaders(appendRecord.Headers),
		Attributes:  appendRecord.Attributes,
		Payload:     cloneBytes(appendRecord.Payload),
	}
}
