package store

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
)

const segmentIndexExtension = ".idx"

func (s *StorxLogStore) loadSegmentIndexes() error {
	loaded := collectionmapping.NewMap[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]]()
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
		return s.loadSegmentIndexFile(relativePath, loaded)
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
		s.nextOffsets.Set(tp, nextOffsetFromPointers(s.records.GetOrDefault(tp, nil)))
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
		applyLoadedSegmentIndexEntry(loaded, entry)
	}
	return nil
}

func applyLoadedSegmentIndexEntry(
	loaded *collectionmapping.Map[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]],
	entry segmentIndexEntry,
) {
	pointer := entry.Pointer
	tp := NewTopicPartition(pointer.Topic, pointer.Partition)
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
	byPath.Range(func(relativePath string, entries []segmentIndexEntry) bool {
		result = errors.Join(result, s.appendSegmentIndexEntries(relativePath, entries))
		return true
	})
	return result
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
	root, err := os.OpenRoot(s.segmentsDir)
	if err != nil {
		return wrapExternal(err, "open segment index root")
	}
	file, err := root.OpenFile(indexRelativePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	closeRootErr := root.Close()
	if err != nil {
		return errors.Join(wrapExternal(err, "open segment index"), wrapExternal(closeRootErr, "close segment index root"))
	}
	if closeRootErr != nil {
		return errors.Join(wrapExternal(closeRootErr, "close segment index root"), wrapExternal(file.Close(), "close segment index"))
	}
	if _, err := file.Write(payload); err != nil {
		return errors.Join(wrapExternal(err, "write segment index"), wrapExternal(file.Close(), "close segment index"))
	}
	if err := file.Sync(); err != nil {
		return errors.Join(wrapExternal(err, "sync segment index"), wrapExternal(file.Close(), "close segment index"))
	}
	return wrapExternal(file.Close(), "close segment index")
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
