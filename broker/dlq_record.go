package broker

import (
	"strings"

	"github.com/lyonbrown4d/ech0/store"
)

func dlqRecordFromStore(dlqTopic string, partition uint32, record store.Record) (DLQRecord, error) {
	originalPartition, err := requiredHeaderUint32(record.Headers, dlqHeaderOriginalPartition)
	if err != nil {
		return DLQRecord{}, err
	}
	originalOffset, err := requiredHeaderUint64(record.Headers, dlqHeaderOriginalOffset)
	if err != nil {
		return DLQRecord{}, err
	}
	retryCount, err := requiredHeaderUint32(record.Headers, dlqHeaderRetryCount)
	if err != nil {
		return DLQRecord{}, err
	}
	return DLQRecord{
		DLQTopic:          dlqTopic,
		DLQPartition:      partition,
		DLQOffset:         record.Offset,
		DLQNextOffset:     record.Offset + 1,
		TimestampMS:       record.TimestampMS,
		Key:               append([]byte(nil), record.Key...),
		Headers:           cloneRecordHeaders(record.Headers),
		Payload:           append([]byte(nil), record.Payload...),
		OriginalTopic:     headerString(record.Headers, dlqHeaderOriginalTopic),
		OriginalPartition: originalPartition,
		OriginalOffset:    originalOffset,
		RetryCount:        retryCount,
		ErrorCode:         headerString(record.Headers, dlqHeaderErrorCode),
		ErrorMessage:      headerString(record.Headers, dlqHeaderErrorMessage),
	}, nil
}

func dlqRecordMatches(query DLQQuery, record DLQRecord) bool {
	return dlqTimestampMatches(query, record) &&
		dlqOriginMatches(query, record) &&
		dlqErrorMatches(query, record) &&
		dlqHeadersMatch(query.Headers, record.Headers)
}

func dlqTimestampMatches(query DLQQuery, record DLQRecord) bool {
	if query.FromTimestampMS != nil && record.TimestampMS < *query.FromTimestampMS {
		return false
	}
	return query.ToTimestampMS == nil || record.TimestampMS <= *query.ToTimestampMS
}

func dlqOriginMatches(query DLQQuery, record DLQRecord) bool {
	if query.OriginalPartition != nil && record.OriginalPartition != *query.OriginalPartition {
		return false
	}
	return query.OriginalOffset == nil || record.OriginalOffset == *query.OriginalOffset
}

func dlqErrorMatches(query DLQQuery, record DLQRecord) bool {
	if query.ErrorCode != "" && record.ErrorCode != query.ErrorCode {
		return false
	}
	if query.RetryCount != nil && record.RetryCount != *query.RetryCount {
		return false
	}
	return query.ErrorMessageContains == "" || strings.Contains(record.ErrorMessage, query.ErrorMessageContains)
}

func dlqHeadersMatch(filters []DLQHeaderFilter, headers []store.RecordHeader) bool {
	for _, filter := range filters {
		if filter.Key == "" {
			continue
		}
		if !dlqHeaderMatches(filter, headers) {
			return false
		}
	}
	return true
}

func dlqHeaderMatches(filter DLQHeaderFilter, headers []store.RecordHeader) bool {
	for _, header := range headers {
		if header.Key != filter.Key {
			continue
		}
		return filter.Value == nil || string(header.Value) == *filter.Value
	}
	return false
}

func dlqReplayAppend(record store.Record) store.RecordAppend {
	appendRecord := cloneAsAppend(record)
	appendRecord.ExpiresAtMS = nil
	appendRecord.Headers = removeHeaders(appendRecord.Headers, internalDLQAndRetryHeaders()...)
	return appendRecord
}

func dlqHasMore(nextOffset uint64, highWatermark *uint64) bool {
	return highWatermark != nil && nextOffset <= *highWatermark
}
