package store

import (
	"context"
	"os"
	"path/filepath"
	"sync"

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
	mu          sync.Mutex
	rootDir     string
	segmentsDir string
	index       *badgerx.DB
	topics      *badgerx.Namespace[string, TopicConfig]
	records     *badgerx.Namespace[string, segmentRecordPointer]
	nextOffsets *badgerx.Namespace[string, uint64]
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

func OpenStorxLogStore(path string) (*StorxLogStore, error) {
	rootDir, normalizeErr := normalizeSegmentRoot(path)
	if normalizeErr != nil {
		return nil, normalizeErr
	}
	segmentsDir := filepath.Join(rootDir, "segments")
	if mkdirErr := os.MkdirAll(segmentsDir, 0o750); mkdirErr != nil {
		return nil, wrapExternal(mkdirErr, "create segment log directory")
	}
	index, err := openSegmentIndex(filepath.Join(rootDir, "index.badger"))
	if err != nil {
		return nil, err
	}
	keyCodec := keycodec.String()
	return &StorxLogStore{
		rootDir:     rootDir,
		segmentsDir: segmentsDir,
		index:       index,
		topics:      badgerx.NewNamespaceWithDB(index, indexLogTopics, keyCodec, codec.JSON[TopicConfig]()),
		records:     badgerx.NewNamespaceWithDB(index, indexLogRecords, keyCodec, codec.JSON[segmentRecordPointer]()),
		nextOffsets: badgerx.NewNamespaceWithDB(index, indexLogOffsets, keyCodec, codec.JSON[uint64]()),
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

func openSegmentIndex(path string) (*badgerx.DB, error) {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return nil, wrapExternal(err, "create segment index directory")
	}
	options := badger.DefaultOptions(path).WithLogger(nil)
	index, err := badgerx.Open(options)
	if err != nil {
		return nil, wrapExternal(err, "open segment index")
	}
	return index, nil
}

func (s *StorxLogStore) Close() error {
	if s == nil || s.index == nil {
		return nil
	}
	return wrapExternal(s.index.Close(), "close segment index")
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

func (s *StorxLogStore) AppendRecord(topicPartition TopicPartition, appendRecord RecordAppend) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, err := s.loadTopicForPartition(topicPartition)
	if err != nil {
		return Record{}, err
	}
	if len(appendRecord.Payload) > int(topic.MaxMessageBytes) {
		return Record{}, E(CodeInvalidArgument, "payload size %d exceeds max_message_bytes %d", len(appendRecord.Payload), topic.MaxMessageBytes)
	}
	offset, err := s.nextOffset(topicPartition)
	if err != nil {
		return Record{}, err
	}
	record := newStoredRecord(offset, appendRecord)
	frame, err := encodeSegmentFrame(record)
	if err != nil {
		return Record{}, err
	}
	pointer, err := s.appendFrame(topic, topicPartition, offset, frame, record)
	if err != nil {
		return Record{}, err
	}
	if err := s.records.Set(context.Background(), recordKey(topicPartition, offset), pointer); err != nil {
		return Record{}, wrapExternal(err, "save segment record index")
	}
	if err := s.nextOffsets.Set(context.Background(), nextOffsetKey(topicPartition), offset+1); err != nil {
		return Record{}, wrapExternal(err, "advance next log offset")
	}
	return cloneRecord(record), nil
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
