//go:build windows

package store

func openMappedSegment(string, string) (*mappedSegment, error) {
	return nil, errSegmentMmapUnsupported
}
