package store

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type StorxLogRepairOptions struct {
	DryRun                bool `json:"dry_run"`
	RebuildCorruptIndexes bool `json:"rebuild_corrupt_indexes"`
}

type StorxLogRepairResult struct {
	CheckedSegments           int      `json:"checked_segments"`
	SkippedSegments           int      `json:"skipped_segments"`
	ExistingIndexes           int      `json:"existing_indexes"`
	MissingIndexes            int      `json:"missing_indexes"`
	CorruptIndexes            int      `json:"corrupt_indexes"`
	RebuiltIndexes            int      `json:"rebuilt_indexes"`
	BackedUpCorruptIndexFiles int      `json:"backed_up_corrupt_index_files"`
	Errors                    []string `json:"errors,omitempty"`
}

func RepairStorxLog(path string, options StorxLogRepairOptions) (StorxLogRepairResult, error) {
	repair, err := newStorxLogRepair(path, options)
	if err != nil {
		return StorxLogRepairResult{}, err
	}
	result := repair.run()
	if len(result.Errors) > 0 {
		return result, E(CodeUnavailable, "segment log repair completed with %d errors", len(result.Errors))
	}
	return result, nil
}

type storxLogRepair struct {
	store       *StorxLogStore
	options     StorxLogRepairOptions
	loaded      *collectionmapping.Map[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]]
	nextOffsets *collectionmapping.Map[TopicPartition, uint64]
	result      StorxLogRepairResult
	errors      *collectionlist.List[string]
}

func newStorxLogRepair(path string, options StorxLogRepairOptions) (*storxLogRepair, error) {
	rootDir, normalizeErr := normalizeSegmentRoot(path)
	if normalizeErr != nil {
		return nil, normalizeErr
	}
	repair := &storxLogRepair{
		store: &StorxLogStore{
			rootDir:          rootDir,
			segmentsDir:      filepath.Join(rootDir, "segments"),
			topics:           collectionmapping.NewMap[string, TopicConfig](),
			records:          collectionmapping.NewMap[TopicPartition, []segmentRecordPointer](),
			timestampRecords: collectionmapping.NewMap[TopicPartition, []segmentRecordPointer](),
			keyRecords:       collectionmapping.NewMap[TopicPartition, *collectionmapping.Map[string, segmentRecordPointer]](),
			keyIndexReady:    collectionset.NewSet[TopicPartition](),
			nextOffsets:      collectionmapping.NewMap[TopicPartition, uint64](),
		},
		options:     options,
		loaded:      collectionmapping.NewMap[TopicPartition, *collectionmapping.Map[uint64, segmentRecordPointer]](),
		nextOffsets: collectionmapping.NewMap[TopicPartition, uint64](),
		errors:      collectionlist.NewList[string](),
	}
	if err := repair.store.loadLogManifest(); err != nil {
		return nil, err
	}
	return repair, nil
}

func (r *storxLogRepair) run() StorxLogRepairResult {
	if err := filepath.WalkDir(r.store.segmentsDir, r.visit); err != nil {
		r.addError(wrapExternal(err, "walk segment log for repair"))
	}
	r.result.Errors = r.errors.Values()
	return r.result
}

func (r *storxLogRepair) visit(path string, entry os.DirEntry, err error) error {
	if err != nil {
		r.addError(wrapExternal(err, "visit segment log path"))
		return nil
	}
	if entry.IsDir() || filepath.Ext(path) != segmentFileExtension {
		return nil
	}
	relativePath, relErr := filepath.Rel(r.store.segmentsDir, path)
	if relErr != nil {
		r.addError(wrapExternal(relErr, "resolve segment path for repair"))
		return nil
	}
	r.repairSegment(relativePath)
	return nil
}

func (r *storxLogRepair) repairSegment(relativePath string) {
	r.result.CheckedSegments++
	tp, _, err := parseSegmentRelativePath(relativePath)
	if err != nil {
		r.addError(err)
		return
	}
	if !r.store.segmentPartitionExists(tp) {
		r.result.SkippedSegments++
		return
	}
	indexRelativePath := segmentIndexRelativePath(relativePath)
	if err := r.validateOrRepairIndex(relativePath, indexRelativePath); err != nil {
		r.addError(err)
	}
}

func (r *storxLogRepair) validateOrRepairIndex(relativePath, indexRelativePath string) error {
	if err := statSegmentIndex(r.store.segmentsDir, indexRelativePath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			r.result.MissingIndexes++
			return r.rebuildMissingIndex(relativePath)
		}
		return wrapExternal(err, "stat segment index for repair")
	}
	if err := validateSegmentIndexFile(r.store.segmentsDir, indexRelativePath); err != nil {
		r.result.CorruptIndexes++
		if !r.options.RebuildCorruptIndexes {
			return err
		}
		return r.rebuildCorruptIndex(relativePath, indexRelativePath)
	}
	r.result.ExistingIndexes++
	return nil
}

func (r *storxLogRepair) rebuildMissingIndex(relativePath string) error {
	if r.options.DryRun {
		return nil
	}
	if err := r.store.rebuildMissingSegmentIndex(relativePath, r.loaded, r.nextOffsets); err != nil {
		return err
	}
	r.result.RebuiltIndexes++
	return nil
}

func (r *storxLogRepair) rebuildCorruptIndex(relativePath, indexRelativePath string) error {
	if r.options.DryRun {
		return nil
	}
	backupPath, err := backupCorruptIndexFile(r.store.segmentsDir, indexRelativePath)
	if err != nil {
		return err
	}
	r.result.BackedUpCorruptIndexFiles++
	if err := r.store.rebuildMissingSegmentIndex(relativePath, r.loaded, r.nextOffsets); err != nil {
		return errors.Join(err, restoreCorruptIndexBackup(r.store.segmentsDir, indexRelativePath, backupPath))
	}
	r.result.RebuiltIndexes++
	return nil
}

func (r *storxLogRepair) addError(err error) {
	if err != nil {
		r.errors.Add(err.Error())
	}
}

func validateSegmentIndexFile(rootDir, indexRelativePath string) error {
	root, err := os.OpenRoot(rootDir)
	if err != nil {
		return wrapExternal(err, "open segment index root")
	}
	file, err := root.Open(indexRelativePath)
	closeRootErr := root.Close()
	if err != nil {
		return errors.Join(wrapExternal(err, "open segment index"), wrapExternal(closeRootErr, "close segment index root"))
	}
	_, readErr := readSegmentIndexEntries(file)
	closeErr := file.Close()
	return errors.Join(readErr, wrapExternal(closeErr, "close segment index"), wrapExternal(closeRootErr, "close segment index root"))
}

func backupCorruptIndexFile(rootDir, indexRelativePath string) (string, error) {
	backupRelativePath := indexRelativePath + ".corrupt." + time.Now().UTC().Format("20060102150405.000000000")
	if err := os.Rename(filepath.Join(rootDir, indexRelativePath), filepath.Join(rootDir, backupRelativePath)); err != nil {
		return "", wrapExternal(err, "backup corrupt segment index")
	}
	return backupRelativePath, nil
}

func restoreCorruptIndexBackup(rootDir, indexRelativePath, backupRelativePath string) error {
	if backupRelativePath == "" {
		return nil
	}
	return wrapExternal(
		os.Rename(filepath.Join(rootDir, backupRelativePath), filepath.Join(rootDir, indexRelativePath)),
		"restore corrupt segment index backup",
	)
}
