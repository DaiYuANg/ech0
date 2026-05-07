package broker

import (
	"strconv"

	"github.com/DaiYuANg/ech0/store"
)

func retryBackoffMS(retryCount uint32, policy store.TopicRetryPolicy) uint64 {
	if policy.BackoffInitialMS == 0 {
		policy = store.DefaultTopicRetryPolicy()
	}
	maxDelay := policy.BackoffMaxMS
	if maxDelay == 0 {
		maxDelay = policy.BackoffInitialMS
	}
	delay := policy.BackoffInitialMS
	for i := uint32(1); i < retryCount; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func retryOriginFromRecord(record store.Record, topic string, partition uint32) (retryOrigin, error) {
	origin := retryOrigin{
		Topic:     valueOr(headerString(record.Headers, retryHeaderOriginalTopic), topic),
		Partition: partition,
		Offset:    record.Offset,
	}
	if parsed, err := headerUint32(record.Headers, retryHeaderOriginalPartition); err != nil {
		return retryOrigin{}, err
	} else if parsed != nil {
		origin.Partition = *parsed
	}
	if parsed, err := headerUint64(record.Headers, retryHeaderOriginalOffset); err != nil {
		return retryOrigin{}, err
	} else if parsed != nil {
		origin.Offset = *parsed
	}
	if parsed, err := headerUint32(record.Headers, retryHeaderRetryCount); err != nil {
		return retryOrigin{}, err
	} else if parsed != nil {
		origin.RetryCount = *parsed
	}
	return origin, nil
}

func buildDLQAppend(retryRecord store.Record, origin retryOrigin) store.RecordAppend {
	errorMessage := valueOr(headerString(retryRecord.Headers, retryHeaderLastError), "unknown")
	appendRecord := store.NewRecordAppend(retryRecord.Payload)
	appendRecord.TimestampMS = &retryRecord.TimestampMS
	appendRecord.Key = append([]byte(nil), retryRecord.Key...)
	appendRecord.Attributes = retryRecord.Attributes
	appendRecord.Headers = []store.RecordHeader{
		header(dlqHeaderOriginalTopic, origin.Topic),
		header(dlqHeaderOriginalPartition, strconv.FormatUint(uint64(origin.Partition), 10)),
		header(dlqHeaderOriginalOffset, strconv.FormatUint(origin.Offset, 10)),
		header(dlqHeaderRetryCount, strconv.FormatUint(uint64(origin.RetryCount), 10)),
		header(dlqHeaderErrorCode, dlqErrorCodeRetryExhausted),
		header(dlqHeaderErrorMessage, errorMessage),
	}
	return appendRecord
}

type retryOrigin struct {
	Topic      string
	Partition  uint32
	Offset     uint64
	RetryCount uint32
}
