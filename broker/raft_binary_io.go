package broker

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/samber/oops"
	"github.com/lyonbrown4d/ech0/store"
)

type raftBinaryWriter struct {
	buf bytes.Buffer
}

func newRaftBinaryWriter() *raftBinaryWriter {
	return &raftBinaryWriter{}
}

func (w *raftBinaryWriter) bytes() []byte {
	return w.buf.Bytes()
}

func (w *raftBinaryWriter) writeRaw(value []byte) {
	if _, err := w.buf.Write(value); err != nil {
		panic(err)
	}
}

func (w *raftBinaryWriter) writeU8(value uint8) {
	if err := w.buf.WriteByte(value); err != nil {
		panic(err)
	}
}

func (w *raftBinaryWriter) writeBool(value bool) {
	if value {
		w.writeU8(1)
		return
	}
	w.writeU8(0)
}

func (w *raftBinaryWriter) writeU16(value uint16) {
	var raw [2]byte
	binary.BigEndian.PutUint16(raw[:], value)
	w.writeRaw(raw[:])
}

func (w *raftBinaryWriter) writeU32(value uint32) {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], value)
	w.writeRaw(raw[:])
}

func (w *raftBinaryWriter) writeU64(value uint64) {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], value)
	w.writeRaw(raw[:])
}

func (w *raftBinaryWriter) writeOptionalU64(value *uint64) {
	if value == nil {
		w.writeBool(false)
		return
	}
	w.writeBool(true)
	w.writeU64(*value)
}

func (w *raftBinaryWriter) writeString(value string) error {
	raw := []byte(value)
	size, err := checkedRaftUint16(len(raw), "string")
	if err != nil {
		return err
	}
	w.writeU16(size)
	w.writeRaw(raw)
	return nil
}

func (w *raftBinaryWriter) writeBytes(value []byte) error {
	size, err := checkedRaftUint32(len(value), "bytes")
	if err != nil {
		return err
	}
	w.writeU32(size)
	w.writeRaw(value)
	return nil
}

type raftBinaryReader struct {
	inner *bytes.Reader
}

func newRaftBinaryReader(data []byte) *raftBinaryReader {
	return &raftBinaryReader{inner: bytes.NewReader(data)}
}

func (r *raftBinaryReader) readU8() (uint8, error) {
	value, err := r.inner.ReadByte()
	if err != nil {
		return 0, wrapBroker("raft_binary_read_failed", err, "read u8")
	}
	return value, nil
}

func (r *raftBinaryReader) readBool() (bool, error) {
	value, err := r.readU8()
	if err != nil {
		return false, err
	}
	return value != 0, nil
}

func (r *raftBinaryReader) readU16() (uint16, error) {
	var raw [2]byte
	if _, err := io.ReadFull(r.inner, raw[:]); err != nil {
		return 0, wrapBroker("raft_binary_read_failed", err, "read u16")
	}
	return binary.BigEndian.Uint16(raw[:]), nil
}

func (r *raftBinaryReader) readU32() (uint32, error) {
	var raw [4]byte
	if _, err := io.ReadFull(r.inner, raw[:]); err != nil {
		return 0, wrapBroker("raft_binary_read_failed", err, "read u32")
	}
	return binary.BigEndian.Uint32(raw[:]), nil
}

func (r *raftBinaryReader) readU64() (uint64, error) {
	var raw [8]byte
	if _, err := io.ReadFull(r.inner, raw[:]); err != nil {
		return 0, wrapBroker("raft_binary_read_failed", err, "read u64")
	}
	return binary.BigEndian.Uint64(raw[:]), nil
}

func (r *raftBinaryReader) readOptionalU64() (*uint64, error) {
	ok, err := r.readBool()
	if err != nil || !ok {
		return nil, err
	}
	value, err := r.readU64()
	return &value, err
}

func (r *raftBinaryReader) readString() (string, error) {
	size, err := r.readU16()
	if err != nil {
		return "", err
	}
	raw, err := r.readFixedBytes(uint32(size))
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func (r *raftBinaryReader) readBytes() ([]byte, error) {
	size, err := r.readU32()
	if err != nil {
		return nil, err
	}
	return r.readFixedBytes(size)
}

func (r *raftBinaryReader) readFixedBytes(size uint32) ([]byte, error) {
	length, err := intFromRaftUint32(size)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, length)
	if length == 0 {
		return raw, nil
	}
	if _, err := io.ReadFull(r.inner, raw); err != nil {
		return nil, wrapBroker("raft_binary_read_failed", err, "read bytes")
	}
	return raw, nil
}

func (r *raftBinaryReader) ensureEOF() error {
	if r.inner.Len() != 0 {
		return brokerStoreError(store.CodeCodec, "raft binary body has %d trailing bytes", r.inner.Len())
	}
	return nil
}

func checkedRaftUint16(value int, label string) (uint16, error) {
	if value < 0 || uint64(value) > uint64(^uint16(0)) {
		return 0, brokerStoreError(store.CodeCodec, "raft binary %s length %d exceeds u16", label, value)
	}
	return uint16(value), nil
}

func checkedRaftUint32(value int, label string) (uint32, error) {
	if value < 0 || uint64(value) > uint64(^uint32(0)) {
		return 0, brokerStoreError(store.CodeCodec, "raft binary %s length %d exceeds u32", label, value)
	}
	return uint32(value), nil
}

func intFromRaftUint32(value uint32) (int, error) {
	maxInt := int(^uint(0) >> 1)
	if value > uint32(maxInt) {
		return 0, oops.In("broker").Code("raft_binary_length_convert_failed").With("value", value, "max", maxInt).New("u32 value exceeds int")
	}
	return int(value), nil
}
