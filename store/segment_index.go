package store

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

const segmentIndexExtension = ".idx"

func (s *StorxLogStore) loadSegmentIndexes() error {
	loaded := collectionmapping.NewMap[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]]()
	nextOffsets := collectionmapping.NewMap[TopicPartition, uint64]()
	if err := filepath.WalkDir(s.segmentsDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != segmentIndexExtension {
			return nil
		}
		relativePath, relErr := filepath.Rel(s.segmentsDir, path)
		if relErr != nil {
			return wrapExternal(relErr, "resolve segment index path")
		}
		return s.loadSegmentIndexFile(relativePath, loaded, nextOffsets)
	}); err != nil {
		return wrapExternal(err, "load segment indexes")
	}
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	loaded.Range(func(tp TopicPartition, byOffset *collectionmapping.Map[uint64, segmentRecordPointer]) bool {
		pointers := collectionlist.NewListWithCapacity[segmentRecordPointer](byOffset.Len())
		byOffset.Range(func(_ uint64, pointer segmentRecordPointer) bool {
			pointers.Add(pointer)
			return true
		})
		s.records.Set(tp, sortSegmentPointers(pointers.Values()))
		s.nextOffsets.Set(tp, max(nextOffsets.GetOrDefault(tp, 0), nextOffsetFromPointers(s.records.GetOrDefault(tp, nil))))
		return true
	})
	nextOffsets.Range(func(tp TopicPartition, next uint64) bool {
		if _, ok := loaded.Get(tp); !ok {
			s.nextOffsets.Set(tp, next)
		}
		return true
	})
	s.topics.Range(func(_ string, topic TopicConfig) bool {
		s.ensureTopicPartitionsLocked(topic)
		return true
	})
	return nil
}

func (s *StorxLogStore) loadSegmentIndexFile(
	relativePath string,
	loaded *collectionmapping.Map[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]],
	nextOffsets *collectionmapping.Map[TopicPartition, uint64],
) error {
	root, err := os.OpenRoot(s.segmentsDir)
	if err != nil {
		return wrapExternal(err, "open segment index root")
	}
	file, err := root.Open(relativePath)
	closeRootErr := root.Close()
	if err != nil {
		return errors.Join(wrapExternal(err, "open segment index"), wrapExternal(closeRootErr, "close segment index root"))
	}
	entries, readErr := readSegmentIndexEntries(file)
	closeErr := file.Close()
	if readErr != nil {
		return errors.Join(readErr, wrapExternal(closeErr, "close segment index"))
	}
	if closeErr != nil {
		return wrapExternal(closeErr, "close segment index")
	}
	if closeRootErr != nil {
		return wrapExternal(closeRootErr, "close segment index root")
	}
	for _, entry := range entries {
		applyLoadedSegmentIndexEntry(loaded, nextOffsets, entry)
	}
	return nil
}

func applyLoadedSegmentIndexEntry(
	loaded *collectionmapping.Map[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]],
	nextOffsets *collectionmapping.Map[TopicPartition, uint64],
	entry segmentIndexEntry,
) {
	pointer := entry.Pointer
	tp := NewTopicPartition(pointer.Topic, pointer.Partition)
	nextOffsets.Set(tp, max(nextOffsets.GetOrDefault(tp, 0), pointer.Offset+1))
	byOffset, ok := loaded.Get(tp)
	if !ok {
		byOffset = collectionmapping.NewMap[uint64, segmentRecordPointer]()
		loaded.Set(tp, byOffset)
	}
	if entry.Deleted {
		byOffset.Delete(pointer.Offset)
		return
	}
	byOffset.Set(pointer.Offset, pointer)
}

func (s *StorxLogStore) appendSegmentIndexPointers(relativePath string, pointers []segmentRecordPointer) error {
	if len(pointers) == 0 {
		return nil
	}
	entries := collectionlist.NewListWithCapacity[segmentIndexEntry](len(pointers))
	for _, pointer := range pointers {
		entries.Add(segmentIndexEntry{Pointer: pointer})
	}
	return s.appendSegmentIndexEntries(relativePath, entries.Values())
}

func (s *StorxLogStore) appendSegmentIndexDeletes(pointers []segmentRecordPointer) error {
	byPath := collectionmapping.NewMap[string, []segmentIndexEntry]()
	for _, pointer := range pointers {
		tp := NewTopicPartition(pointer.Topic, pointer.Partition)
		path := s.segmentRelativePath(tp, pointer.SegmentID)
		byPath.Set(path, append(byPath.GetOrDefault(path, nil), segmentIndexEntry{Pointer: pointer, Deleted: true}))
	}
	var result error
	indexPaths := collectionlist.NewListWithCapacity[string](byPath.Len())
	byPath.Range(func(relativePath string, entries []segmentIndexEntry) bool {
		result = errors.Join(result, s.appendSegmentIndexEntries(relativePath, entries))
		indexPaths.Add(segmentIndexRelativePath(relativePath))
		return true
	})
	return errors.Join(result, s.syncAppendWrites(nil, indexPaths.Values()))
}

func (s *StorxLogStore) appendSegmentIndexEntries(relativePath string, entries []segmentIndexEntry) error {
	payload, err := encodeSegmentIndexEntries(entries)
	if err != nil {
		return err
	}
	indexRelativePath := segmentIndexRelativePath(relativePath)
	indexDir := filepath.Join(s.segmentsDir, filepath.Dir(indexRelativePath))
	if mkdirErr := os.MkdirAll(indexDir, 0o750); mkdirErr != nil {
		return wrapExternal(mkdirErr, "create segment index directory")
	}
	writer, err := s.segmentIndexWriter(indexRelativePath)
	if err != nil {
		return err
	}
	return writer.append(payload)
}

type segmentIndexWriter struct {
	mu   sync.Mutex
	file *os.File
}

func (s *StorxLogStore) segmentIndexWriter(indexRelativePath string) (*segmentIndexWriter, error) {
	s.indexWritersMu.Lock()
	defer s.indexWritersMu.Unlock()
	if writer, ok := s.indexWriters.Get(indexRelativePath); ok {
		return writer, nil
	}
	writer, err := openSegmentIndexWriter(s.segmentsDir, indexRelativePath)
	if err != nil {
		return nil, err
	}
	s.indexWriters.Set(indexRelativePath, writer)
	return writer, nil
}

func openSegmentIndexWriter(rootDir, indexRelativePath string) (*segmentIndexWriter, error) {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return nil, wrapExternal(err, "open segment index root")
	}
	file, err := root.OpenFile(indexRelativePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	closeRootErr := root.Close()
	if err != nil {
		return nil, errors.Join(wrapExternal(err, "open segment index"), wrapExternal(closeRootErr, "close segment index root"))
	}
	if closeRootErr != nil {
		return nil, errors.Join(wrapExternal(closeRootErr, "close segment index root"), wrapExternal(file.Close(), "close segment index"))
	}
	return &segmentIndexWriter{file: file}, nil
}

func (w *segmentIndexWriter) append(payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	written, err := w.file.Write(payload)
	if err != nil {
		return wrapExternal(err, "write segment index")
	}
	if written != len(payload) {
		return wrapExternal(io.ErrShortWrite, "write segment index")
	}
	return nil
}

func (w *segmentIndexWriter) sync() error {
	if w == nil || w.file == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return wrapExternal(w.file.Sync(), "sync segment index")
}

func (s *StorxLogStore) closeSegmentIndexWriters() error {
	if s == nil || s.indexWriters == nil {
		return nil
	}
	s.indexWritersMu.Lock()
	defer s.indexWritersMu.Unlock()
	var result error
	s.indexWriters.Range(func(_ string, writer *segmentIndexWriter) bool {
		if writer != nil && writer.file != nil {
			result = errors.Join(result, writer.sync(), wrapExternal(writer.file.Close(), "close segment index"))
		}
		return true
	})
	s.indexWriters.Clear()
	return result
}

func segmentIndexRelativePath(segmentRelativePath string) string {
	extension := filepath.Ext(segmentRelativePath)
	if extension == "" {
		return segmentRelativePath + segmentIndexExtension
	}
	return strings.TrimSuffix(segmentRelativePath, extension) + segmentIndexExtension
}

func sortSegmentPointers(pointers []segmentRecordPointer) []segmentRecordPointer {
	sort.Slice(pointers, func(i, j int) bool {
		return pointers[i].Offset < pointers[j].Offset
	})
	return pointers
}

func nextOffsetFromPointers(pointers []segmentRecordPointer) uint64 {
	next := uint64(0)
	for _, pointer := range pointers {
		if pointer.Offset >= next {
			next = pointer.Offset + 1
		}
	}
	return next
}
