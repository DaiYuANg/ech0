package store

import "errors"

var errSegmentMmapUnsupported = errors.New("segment mmap is unsupported on this platform")

type mappedSegment struct {
	data  []byte
	close func() error
}

func closeMappedSegment(mapped *mappedSegment) error {
	if mapped == nil || mapped.close == nil {
		return nil
	}
	return mapped.close()
}
