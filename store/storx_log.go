package store

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

type StorxLogStore struct {
	mu               sync.Mutex
	indexMu          sync.RWMutex
	partitionLocksMu sync.Mutex
	writersMu        sync.Mutex
	readersMu        sync.Mutex
	rootDir          string
	segmentsDir      string
	compression      segmentFrameCompression
	topics           *collectionmapping.Map[string, TopicConfig]
	records          *collectionmapping.Map[TopicPartition, []segmentRecordPointer]
	nextOffsets      *collectionmapping.Map[TopicPartition, uint64]
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
	store := &StorxLogStore{
		rootDir:        rootDir,
		segmentsDir:    segmentsDir,
		compression:    compression,
		topics:         collectionmapping.NewMap[string, TopicConfig](),
		records:        collectionmapping.NewMap[TopicPartition, []segmentRecordPointer](),
		nextOffsets:    collectionmapping.NewMap[TopicPartition, uint64](),
		metrics:        options.Metrics,
		partitionLocks: collectionmapping.NewMap[TopicPartition, *sync.Mutex](),
		writers:        collectionmapping.NewMap[string, *segmentWriter](),
		readers:        collectionmapping.NewMap[string, *segmentReader](),
	}
	if err := store.loadLogManifest(); err != nil {
		return nil, errors.Join(err, compression.close())
	}
	if err := store.loadSegmentIndexes(); err != nil {
		return nil, errors.Join(err, compression.close())
	}
	return store, nil
}

func (s *StorxLogStore) Close() error {
	if s == nil {
		return nil
	}
	return errors.Join(
		s.closeSegmentWriters(),
		s.closeSegmentReaders(),
		s.compression.close(),
	)
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

func (s *StorxLogStore) CreateTopic(topic TopicConfig) error {
	if err := validateLogTopicForCreate(&topic); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.registerLogTopic(topic); err != nil {
		return err
	}
	if err := s.persistLogManifest(); err != nil {
		return err
	}
	return s.createLogTopicDirs(topic)
}

func validateLogTopicForCreate(topic *TopicConfig) error {
	if topic.Name == "" {
		return E(CodeInvalidArgument, "topic name is required")
	}
	if topic.Partitions == 0 {
		return E(CodeInvalidArgument, "topic %s must have at least one partition", topic.Name)
	}
	normalizeTopic(topic)
	return nil
}

func (s *StorxLogStore) registerLogTopic(topic TopicConfig) error {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	if _, exists := s.topics.Get(topic.Name); exists {
		return E(CodeTopicExists, "topic %s already exists", topic.Name)
	}
	s.topics.Set(topic.Name, cloneTopic(topic))
	s.ensureTopicPartitionsLocked(topic)
	return nil
}

func (s *StorxLogStore) createLogTopicDirs(topic TopicConfig) error {
	for partition := range topic.Partitions {
		if err := os.MkdirAll(s.partitionDir(NewTopicPartition(topic.Name, partition)), 0o750); err != nil {
			return wrapExternal(err, "create segment partition directory")
		}
	}
	return nil
}

func (s *StorxLogStore) TopicExists(topic string) (bool, error) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	_, exists := s.topics.Get(topic)
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
	offset := s.nextOffset(topicPartition)
	s.recordAppendStage(operation, "next_offset", 1, offsetStart, nil)
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
	indexErr := s.appendSegmentIndexPointers(s.segmentRelativePath(topicPartition, pointer.SegmentID), []segmentRecordPointer{pointer})
	s.recordAppendStage(operation, "index_set", 1, indexStart, indexErr)
	if indexErr != nil {
		return Record{}, wrapExternal(indexErr, "save segment record index")
	}
	nextOffsetStart := time.Now()
	s.recordAppendedPointer(topicPartition, pointer)
	s.recordAppendStage(operation, "next_offset_set", 1, nextOffsetStart, nil)
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
