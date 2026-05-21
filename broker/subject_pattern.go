package broker

import (
	"context"
	"slices"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

const subjectTokenSeparator = "."

type SubjectPatternPollItem struct {
	Subject   string
	Partition uint32
	Poll      store.PollResult
}

type SubjectPatternPollResult struct {
	Pattern string
	Items   []SubjectPatternPollItem
}

func SubjectPatternMatches(pattern, subject string) (bool, error) {
	patternTokens, err := subjectPatternTokens(pattern)
	if err != nil {
		return false, err
	}
	subjectTokens, err := subjectTokens(subject, "subject")
	if err != nil {
		return false, err
	}
	return matchSubjectTokens(patternTokens, subjectTokens), nil
}

func (b *Broker) ListTopicsByPatternFor(ctx context.Context, pattern string) ([]store.TopicConfig, error) {
	pattern = strings.TrimSpace(pattern)
	topics, err := b.ListTopicsFor(ctx)
	if err != nil || pattern == "" {
		return topics, err
	}
	tokens, err := subjectPatternTokens(pattern)
	if err != nil {
		return nil, err
	}
	out := collectionlist.NewListWithCapacity[store.TopicConfig](len(topics))
	for index := range topics {
		topic := topics[index]
		subject, tokenErr := subjectTokens(topic.Name, "subject")
		if tokenErr != nil {
			continue
		}
		if matchSubjectTokens(tokens, subject) {
			out.Add(topic)
		}
	}
	return out.Values(), nil
}

func (b *Broker) FetchSubjectPatternWithIsolation(
	ctx context.Context,
	consumer string,
	pattern string,
	maxRecordsPerPartition int,
	isolation FetchIsolation,
) (SubjectPatternPollResult, error) {
	topics, err := b.ListTopicsByPatternFor(ctx, pattern)
	if err != nil {
		return SubjectPatternPollResult{}, err
	}
	items := collectionlist.NewList[SubjectPatternPollItem]()
	for topicIndex := range topics {
		topic := topics[topicIndex]
		for partition := range topic.Partitions {
			poll, fetchErr := b.FetchWithIsolation(ctx, consumer, topic.Name, partition, nil, maxRecordsPerPartition, isolation)
			if fetchErr != nil {
				return SubjectPatternPollResult{}, fetchErr
			}
			items.Add(SubjectPatternPollItem{Subject: topic.Name, Partition: partition, Poll: poll})
		}
	}
	return SubjectPatternPollResult{Pattern: pattern, Items: items.Values()}, nil
}

func subjectPatternTokens(pattern string) ([]string, error) {
	tokens, err := subjectTokens(pattern, "subject pattern")
	if err != nil {
		return nil, err
	}
	last := len(tokens) - 1
	for index, token := range tokens {
		switch {
		case token == ">" && index != last:
			return nil, brokerStoreError(store.CodeInvalidArgument, "subject pattern > wildcard must be the final token")
		case token == "*" || token == ">":
			continue
		case strings.Contains(token, "*") || strings.Contains(token, ">"):
			return nil, brokerStoreError(store.CodeInvalidArgument, "subject pattern wildcard must occupy a full token")
		}
	}
	return tokens, nil
}

func subjectTokens(subject, label string) ([]string, error) {
	subject = strings.TrimSpace(subject)
	if subject == "" {
		return nil, brokerStoreError(store.CodeInvalidArgument, "%s is required", label)
	}
	tokens := strings.Split(subject, subjectTokenSeparator)
	if slices.Contains(tokens, "") {
		return nil, brokerStoreError(store.CodeInvalidArgument, "%s contains an empty token", label)
	}
	return tokens, nil
}

func matchSubjectTokens(pattern, subject []string) bool {
	patternIndex := 0
	subjectIndex := 0
	for patternIndex < len(pattern) {
		token := pattern[patternIndex]
		if token == ">" {
			return subjectIndex < len(subject)
		}
		if subjectIndex >= len(subject) {
			return false
		}
		if token != "*" && token != subject[subjectIndex] {
			return false
		}
		patternIndex++
		subjectIndex++
	}
	return subjectIndex == len(subject)
}
