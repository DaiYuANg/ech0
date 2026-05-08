package broker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/arcgolabs/storx/bboltx"
	storxcodec "github.com/arcgolabs/storx/codec"
	"github.com/arcgolabs/storx/keycodec"
	msgpackcodec "github.com/hashicorp/go-msgpack/codec"
	hashiraft "github.com/hashicorp/raft"
)

const (
	raftBoltFileMode   = 0o600
	raftBoltLogsBucket = "logs"
	raftBoltConfBucket = "conf"
	raftBoltUint64Size = 8
)

var (
	errRaftBoltKeyNotFound = errors.New("not found")

	_ hashiraft.LogStore    = (*raftBoltStore)(nil)
	_ hashiraft.StableStore = (*raftBoltStore)(nil)
)

type raftBoltStore struct {
	db      *bboltx.DB
	logs    *bboltx.Bucket[uint64, []byte]
	conf    *bboltx.Bucket[[]byte, []byte]
	metrics *MetricsRuntime
}

func openRaftBoltStore(ctx context.Context, path string, metrics *MetricsRuntime) (*raftBoltStore, error) {
	db, err := bboltx.Open(path, raftBoltFileMode, nil)
	if err != nil {
		return nil, wrapBroker("raft_bolt_store_open_failed", err, "open raft bolt store")
	}
	store := &raftBoltStore{
		db:      db,
		logs:    bboltx.NewBucketWithDB[uint64, []byte](db, raftBoltLogsBucket, keycodec.Uint64BE(), storxcodec.Bytes()),
		conf:    bboltx.NewBucketWithDB[[]byte, []byte](db, raftBoltConfBucket, keycodec.Bytes(), storxcodec.Bytes()),
		metrics: metrics,
	}
	if err := store.initialize(ctx); err != nil {
		return nil, errors.Join(err, store.Close())
	}
	return store, nil
}

func (s *raftBoltStore) initialize(ctx context.Context) error {
	if err := s.logs.Update(ctx, func(tx bboltx.UpdateTx[uint64, []byte]) error {
		_ = tx
		return nil
	}); err != nil {
		return wrapBroker("raft_bolt_logs_bucket_create_failed", err, "create raft logs bucket")
	}
	if err := s.conf.Update(ctx, func(tx bboltx.UpdateTx[[]byte, []byte]) error {
		_ = tx
		return nil
	}); err != nil {
		return wrapBroker("raft_bolt_conf_bucket_create_failed", err, "create raft config bucket")
	}
	return nil
}

func (s *raftBoltStore) Close() error {
	if err := s.db.Close(); err != nil {
		return wrapBroker("raft_bolt_store_close_failed", err, "close raft bolt store")
	}
	return nil
}

func (s *raftBoltStore) FirstIndex() (uint64, error) {
	entry, ok, err := s.logs.First(context.Background())
	if err != nil {
		return 0, wrapBroker("raft_bolt_first_index_failed", err, "read first raft index")
	}
	if !ok {
		return 0, nil
	}
	return entry.Key, nil
}

func (s *raftBoltStore) LastIndex() (uint64, error) {
	entry, ok, err := s.logs.Last(context.Background())
	if err != nil {
		return 0, wrapBroker("raft_bolt_last_index_failed", err, "read last raft index")
	}
	if !ok {
		return 0, nil
	}
	return entry.Key, nil
}

func (s *raftBoltStore) GetLog(index uint64, out *hashiraft.Log) error {
	value, ok, err := s.logs.Get(context.Background(), index)
	if err != nil {
		return wrapBroker("raft_bolt_get_log_failed", err, "get raft log")
	}
	if !ok {
		return hashiraft.ErrLogNotFound
	}
	if err := raftBoltDecodeMsgPack(value, out); err != nil {
		return wrapBroker("raft_bolt_log_decode_failed", err, "decode raft log")
	}
	return nil
}

func (s *raftBoltStore) StoreLog(log *hashiraft.Log) error {
	return s.StoreLogs([]*hashiraft.Log{log})
}

func (s *raftBoltStore) StoreLogs(logs []*hashiraft.Log) error {
	totalStart := time.Now()
	var resultErr error
	var totalBytes int
	defer func() {
		s.recordStage("logs", "total", len(logs), totalBytes, totalStart, resultErr)
	}()
	encodeStart := time.Now()
	entries := collectionlist.NewListWithCapacity[bboltx.Entry[uint64, []byte]](len(logs))
	for _, log := range logs {
		encoded, err := raftBoltEncodeMsgPack(log)
		if err != nil {
			resultErr = wrapBroker("raft_bolt_log_encode_failed", err, "encode raft log")
			s.recordStage("logs", "encode", entries.Len(), totalBytes, encodeStart, resultErr)
			return resultErr
		}
		totalBytes += len(encoded)
		entries.Add(bboltx.Entry[uint64, []byte]{Key: log.Index, Value: encoded})
	}
	s.recordStage("logs", "encode", len(logs), totalBytes, encodeStart, nil)
	putStart := time.Now()
	if err := s.logs.PutMany(context.Background(), entries.Values()...); err != nil {
		resultErr = wrapBroker("raft_bolt_store_logs_failed", err, "store raft logs")
		s.recordStage("logs", "put_many", len(logs), totalBytes, putStart, resultErr)
		return resultErr
	}
	s.recordStage("logs", "put_many", len(logs), totalBytes, putStart, nil)
	return nil
}

func (s *raftBoltStore) DeleteRange(minIndex, maxIndex uint64) error {
	if minIndex > maxIndex {
		return nil
	}
	err := s.logs.Update(context.Background(), func(tx bboltx.UpdateTx[uint64, []byte]) error {
		keys, err := raftBoltRangeKeys(tx, minIndex, maxIndex)
		if err != nil {
			return err
		}
		if err := tx.DeleteMany(keys.Values()...); err != nil {
			return wrapBroker("raft_bolt_log_delete_failed", err, "delete raft log")
		}
		return nil
	})
	if err != nil {
		return wrapBroker("raft_bolt_delete_range_failed", err, "delete raft log range")
	}
	return nil
}

func raftBoltRangeKeys(tx bboltx.UpdateTx[uint64, []byte], minIndex, maxIndex uint64) (*collectionlist.List[uint64], error) {
	keys := collectionlist.NewList[uint64]()
	cursor := tx.Cursor()
	index, _, ok, err := cursor.Seek(raftBoltUint64ToBytes(minIndex))
	if err != nil {
		return nil, wrapBroker("raft_bolt_log_cursor_seek_failed", err, "seek raft log delete range")
	}
	for ok && index <= maxIndex {
		keys.Add(index)
		index, _, ok, err = cursor.Next()
		if err != nil {
			return nil, wrapBroker("raft_bolt_log_cursor_next_failed", err, "scan raft log delete range")
		}
	}
	return keys, nil
}

func (s *raftBoltStore) Set(key, value []byte) error {
	start := time.Now()
	if err := s.conf.Put(context.Background(), key, value); err != nil {
		wrapped := wrapBroker("raft_bolt_set_failed", err, "set raft stable value")
		s.recordStage("stable", "put", 1, len(value), start, wrapped)
		s.recordStage("stable", "total", 1, len(value), start, wrapped)
		return wrapped
	}
	s.recordStage("stable", "put", 1, len(value), start, nil)
	s.recordStage("stable", "total", 1, len(value), start, nil)
	return nil
}

func (s *raftBoltStore) Get(key []byte) ([]byte, error) {
	value, ok, err := s.conf.Get(context.Background(), key)
	if err != nil {
		return nil, wrapBroker("raft_bolt_get_failed", err, "get raft stable value")
	}
	if !ok {
		return nil, errRaftBoltKeyNotFound
	}
	return value, nil
}

func (s *raftBoltStore) SetUint64(key []byte, value uint64) error {
	return s.Set(key, raftBoltUint64ToBytes(value))
}

func (s *raftBoltStore) GetUint64(key []byte) (uint64, error) {
	value, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if len(value) != raftBoltUint64Size {
		return 0, wrapBroker("raft_bolt_uint64_decode_failed", errors.New("invalid raft stable uint64 value"), "decode raft stable uint64: expected %d bytes got %d", raftBoltUint64Size, len(value))
	}
	return raftBoltBytesToUint64(value), nil
}

func raftBoltDecodeMsgPack(data []byte, out any) error {
	reader := bytes.NewBuffer(data)
	handle := msgpackcodec.MsgpackHandle{}
	if err := msgpackcodec.NewDecoder(reader, &handle).Decode(out); err != nil {
		return wrapBroker("raft_bolt_msgpack_decode_failed", err, "decode raft msgpack")
	}
	return nil
}

func raftBoltEncodeMsgPack(value any) ([]byte, error) {
	buffer := bytes.NewBuffer(nil)
	handle := msgpackcodec.MsgpackHandle{}
	if err := msgpackcodec.NewEncoder(buffer, &handle).Encode(value); err != nil {
		return nil, wrapBroker("raft_bolt_msgpack_encode_failed", err, "encode raft msgpack")
	}
	return buffer.Bytes(), nil
}

func raftBoltBytesToUint64(data []byte) uint64 {
	return binary.BigEndian.Uint64(data)
}

func raftBoltUint64ToBytes(value uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, value)
	return data
}

func (s *raftBoltStore) recordStage(operation, stage string, entries, payloadBytes int, start time.Time, err error) {
	if s == nil || s.metrics == nil {
		return
	}
	s.metrics.RecordRaftStoreStage(context.Background(), operation, stage, entries, payloadBytes, time.Since(start), err)
}
