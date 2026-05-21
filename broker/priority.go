package broker

import (
	"cmp"
	"strconv"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

const PriorityHeader = "x-ech0-priority"

func appendPriority(record store.RecordAppend) (uint8, bool, error) {
	return priorityFromHeaders(record.Headers)
}

func applyPriority(record *store.RecordAppend, priority uint8) {
	upsertHeader(&record.Headers, PriorityHeader, strconv.FormatUint(uint64(priority), 10))
}

func normalizeRecordPriority(topic store.TopicConfig, record *store.RecordAppend) error {
	policy := store.NormalizeTopicPriorityPolicy(topic.PriorityPolicy)
	if !policy.Enabled {
		return nil
	}
	priority, ok, err := appendPriority(*record)
	if err != nil {
		return err
	}
	if !ok {
		priority = policy.Default
	}
	if priority < policy.Min || priority > policy.Max {
		return brokerStoreError(store.CodeInvalidArgument, "message priority %d is outside topic %s range %d..%d", priority, topic.Name, policy.Min, policy.Max)
	}
	applyPriority(record, priority)
	return nil
}

func normalizeRecordsPriority(topic store.TopicConfig, records []store.RecordAppend) ([]store.RecordAppend, error) {
	if !store.NormalizeTopicPriorityPolicy(topic.PriorityPolicy).Enabled {
		return records, nil
	}
	out := collectionlist.NewListWithCapacity[store.RecordAppend](len(records))
	for index := range records {
		record := records[index]
		if err := normalizeRecordPriority(topic, &record); err != nil {
			return nil, err
		}
		out.Add(record)
	}
	return out.Values(), nil
}

func (b *Broker) normalizeRecordPriorityForTopic(topicName string, record store.RecordAppend) (store.RecordAppend, error) {
	topic, err := b.loadTopicConfig(topicName)
	if err != nil {
		return store.RecordAppend{}, err
	}
	normalizeErr := normalizeRecordPriority(*topic, &record)
	if normalizeErr != nil {
		return store.RecordAppend{}, normalizeErr
	}
	return record, nil
}

func (b *Broker) normalizeRecordsPriorityForTopic(topicName string, records []store.RecordAppend) ([]store.RecordAppend, error) {
	topic, err := b.loadTopicConfig(topicName)
	if err != nil {
		return nil, err
	}
	return normalizeRecordsPriority(*topic, records)
}

func (b *Broker) orderRecordsByPriority(topicName string, records []store.Record) ([]store.Record, error) {
	if len(records) < 2 {
		return records, nil
	}
	topic, err := b.loadTopicConfig(topicName)
	if err != nil {
		return nil, err
	}
	policy := store.NormalizeTopicPriorityPolicy(topic.PriorityPolicy)
	if !policy.Enabled {
		return records, nil
	}
	return orderRecordsByPriorityPolicy(policy, records)
}

type prioritizedRecord struct {
	record   store.Record
	priority uint8
}

func orderRecordsByPriorityPolicy(policy store.TopicPriorityPolicy, records []store.Record) ([]store.Record, error) {
	items := collectionlist.NewListWithCapacity[prioritizedRecord](len(records))
	for index := range records {
		record := records[index]
		priority, err := recordPriority(policy, record)
		if err != nil {
			return nil, err
		}
		items.Add(prioritizedRecord{record: record, priority: priority})
	}
	orderedItems := items.Sort(comparePrioritizedRecord).Values()
	out := collectionlist.NewListWithCapacity[store.Record](len(orderedItems))
	for index := range orderedItems {
		out.Add(orderedItems[index].record)
	}
	return out.Values(), nil
}

func comparePrioritizedRecord(left, right prioritizedRecord) int {
	if left.priority != right.priority {
		return cmp.Compare(right.priority, left.priority)
	}
	return cmp.Compare(left.record.Offset, right.record.Offset)
}

func recordPriority(policy store.TopicPriorityPolicy, record store.Record) (uint8, error) {
	priority, ok, err := priorityFromHeaders(record.Headers)
	if err != nil {
		return 0, err
	}
	if !ok {
		return policy.Default, nil
	}
	if priority < policy.Min || priority > policy.Max {
		return 0, brokerStoreError(store.CodeCodec, "record offset %d priority %d is outside topic range %d..%d", record.Offset, priority, policy.Min, policy.Max)
	}
	return priority, nil
}

func priorityFromHeaders(headers []store.RecordHeader) (uint8, bool, error) {
	for index := range headers {
		header := headers[index]
		if header.Key != PriorityHeader {
			continue
		}
		parsed, err := strconv.ParseUint(string(header.Value), 10, 8)
		if err != nil {
			return 0, true, brokerStoreError(store.CodeCodec, "invalid priority header %s: %v", PriorityHeader, err)
		}
		return uint8(parsed), true, nil
	}
	return 0, false, nil
}

func validateTopicPriorityPolicy(topic store.TopicConfig) error {
	policy := store.NormalizeTopicPriorityPolicy(topic.PriorityPolicy)
	if !policy.Enabled {
		return nil
	}
	if policy.Min > policy.Max {
		return brokerStoreError(store.CodeInvalidArgument, "topic %s priority min %d exceeds max %d", topic.Name, policy.Min, policy.Max)
	}
	if policy.Default < policy.Min || policy.Default > policy.Max {
		return brokerStoreError(store.CodeInvalidArgument, "topic %s priority default %d is outside range %d..%d", topic.Name, policy.Default, policy.Min, policy.Max)
	}
	return nil
}

func topicPriorityPolicyName(policy store.TopicPriorityPolicy) string {
	if store.NormalizeTopicPriorityPolicy(policy).Enabled {
		return "enabled"
	}
	return ""
}
