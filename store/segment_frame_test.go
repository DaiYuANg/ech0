package store_test

import (
	"encoding/base64"
	"encoding/binary"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/lyonbrown4d/ech0/store"
)

const (
	testSegmentFrameMagic     uint32 = 0x45434831
	testSegmentFrameZstdMagic uint32 = 0x45435a31
)

func TestStorxLogStoreDefaultsToZstdFrame(t *testing.T) {
	root := filepath.Join(t.TempDir(), "log")
	st, err := store.OpenStorxLogStore(root)
	requireNoError(t, err)
	defer closeLogStore(t, st)
	topic := store.NewTopicConfig("orders")
	requireNoError(t, st.CreateTopic(topic))
	_, err = st.Append(store.NewTopicPartition(topic.Name, 0), []byte("m1"))
	requireNoError(t, err)
	if magic := readSegmentFrameMagic(t, root, topic.Name, 0, 0); magic != testSegmentFrameZstdMagic {
		t.Fatalf("unexpected default segment frame magic: %x", magic)
	}
}

func TestStorxLogStoreReadsUncompressedFrame(t *testing.T) {
	root := filepath.Join(t.TempDir(), "log")
	st, err := store.OpenStorxLogStoreWithOptions(root, store.StorxLogOptions{Compression: store.SegmentCompressionNone})
	requireNoError(t, err)
	topic := store.NewTopicConfig("orders")
	requireNoError(t, st.CreateTopic(topic))
	_, err = st.Append(store.NewTopicPartition(topic.Name, 0), []byte("legacy"))
	requireNoError(t, err)
	if magic := readSegmentFrameMagic(t, root, topic.Name, 0, 0); magic != testSegmentFrameMagic {
		t.Fatalf("unexpected segment frame magic: %x", magic)
	}
	closeLogStore(t, st)

	st, err = store.OpenStorxLogStore(root)
	requireNoError(t, err)
	defer closeLogStore(t, st)
	records, err := st.ReadFrom(store.NewTopicPartition(topic.Name, 0), 0, 10)
	requireNoError(t, err)
	if len(records) != 1 || string(records[0].Payload) != "legacy" {
		t.Fatalf("unexpected legacy records: %#v", records)
	}
}

func readSegmentFrameMagic(t *testing.T, root, topic string, partition uint32, segmentID uint64) uint32 {
	t.Helper()
	data, err := os.ReadFile(segmentFramePath(root, topic, partition, segmentID))
	requireNoError(t, err)
	if len(data) < 4 {
		t.Fatalf("segment frame too short: %d", len(data))
	}
	return binary.BigEndian.Uint32(data[:4])
}

func segmentFramePath(root, topic string, partition uint32, segmentID uint64) string {
	topicDir := base64.RawURLEncoding.EncodeToString([]byte(topic))
	partitionDir := strconv.FormatUint(uint64(partition), 10)
	return filepath.Join(root, "segments", topicDir, partitionDir, zeroPaddedSegmentID(segmentID)+".seg")
}

func zeroPaddedSegmentID(segmentID uint64) string {
	raw := strconv.FormatUint(segmentID, 10)
	if len(raw) >= 20 {
		return raw
	}
	return strings.Repeat("0", 20-len(raw)) + raw
}
