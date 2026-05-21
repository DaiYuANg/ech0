package ech0

import (
	"context"

	internalbroker "github.com/lyonbrown4d/ech0/broker"
	"github.com/samber/oops"
)

type Subject struct {
	Name       string
	Partitions uint32
}

type SubjectFetchResult struct {
	Pattern string
	Items   []SubjectFetchItem
}

type SubjectFetchItem struct {
	Subject        string
	Partition      uint32
	Messages       []Message
	NextOffset     uint64
	HighWatermark  *uint64
	LowWatermark   *uint64
	LogStartOffset uint64
}

func SubjectPatternMatches(pattern, subject string) (bool, error) {
	matched, err := internalbroker.SubjectPatternMatches(pattern, subject)
	if err != nil {
		return false, oops.In("embedded").Code("subject_pattern_match_failed").With("pattern", pattern, "subject", subject).Wrapf(err, "match subject pattern")
	}
	return matched, nil
}

func (b *Broker) ListSubjects(ctx context.Context, pattern string) ([]Subject, error) {
	topics, err := b.broker.ListTopicsByPatternFor(ctx, pattern)
	if err != nil {
		return nil, oops.In("embedded").Code("subject_list_failed").With("pattern", pattern).Wrapf(err, "list subjects")
	}
	out := make([]Subject, 0, len(topics))
	for index := range topics {
		topic := topics[index]
		out = append(out, Subject{Name: topic.Name, Partitions: topic.Partitions})
	}
	return out, nil
}

func (b *Broker) FetchSubjects(ctx context.Context, consumer, pattern string, opts ...FetchOption) (SubjectFetchResult, error) {
	fetchOpts := fetchOptions{maxRecords: 100}
	for _, opt := range opts {
		if opt != nil {
			opt(&fetchOpts)
		}
	}
	result, err := b.broker.FetchSubjectPatternWithIsolation(ctx, consumer, pattern, fetchOpts.maxRecords, fetchOpts.isolation)
	if err != nil {
		return SubjectFetchResult{}, oops.In("embedded").Code("subject_fetch_failed").With("pattern", pattern).Wrapf(err, "fetch subject pattern")
	}
	return subjectFetchResultFromBroker(result), nil
}

func subjectFetchResultFromBroker(result internalbroker.SubjectPatternPollResult) SubjectFetchResult {
	items := make([]SubjectFetchItem, 0, len(result.Items))
	for index := range result.Items {
		item := &result.Items[index]
		items = append(items, subjectFetchItemFromBroker(item))
	}
	return SubjectFetchResult{Pattern: result.Pattern, Items: items}
}

func subjectFetchItemFromBroker(item *internalbroker.SubjectPatternPollItem) SubjectFetchItem {
	messages := make([]Message, 0, len(item.Poll.Records))
	for index := range item.Poll.Records {
		record := item.Poll.Records[index]
		messages = append(messages, messageFromRecord(item.Subject, item.Partition, record))
	}
	return SubjectFetchItem{
		Subject:        item.Subject,
		Partition:      item.Partition,
		Messages:       messages,
		NextOffset:     item.Poll.NextOffset,
		HighWatermark:  item.Poll.HighWatermark,
		LowWatermark:   item.Poll.LowWatermark,
		LogStartOffset: item.Poll.LogStartOffset,
	}
}
