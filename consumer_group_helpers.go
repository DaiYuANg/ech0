package ech0

import (
	"cmp"

	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/store"
)

func groupOwnedTopicPartitions(assignment store.ConsumerGroupAssignment, memberID string) []TopicPartition {
	out := collectionlist.NewList[TopicPartition]()
	for _, item := range assignment.Assignments {
		if item.MemberID == memberID {
			out.Add(TopicPartition{Topic: item.Topic, Partition: item.Partition})
		}
	}
	return sortTopicPartitions(out.Values())
}

type topicPartitionKey struct {
	topic     string
	partition uint32
}

func partitionDiff(left, right []TopicPartition) []TopicPartition {
	rightIndex := topicPartitionIndex(right)
	out := collectionlist.NewList[TopicPartition]()
	for _, item := range left {
		if _, ok := rightIndex.Get(topicPartitionKey{topic: item.Topic, partition: item.Partition}); !ok {
			out.Add(item)
		}
	}
	return sortTopicPartitions(out.Values())
}

func topicPartitionIndex(partitions []TopicPartition) *collectionmapping.Map[topicPartitionKey, TopicPartition] {
	index := collectionmapping.NewMapWithCapacity[topicPartitionKey, TopicPartition](len(partitions))
	for _, item := range partitions {
		index.Set(topicPartitionKey{topic: item.Topic, partition: item.Partition}, item)
	}
	return index
}

func cloneTopicPartitions(partitions []TopicPartition) []TopicPartition {
	return collectionlist.NewList(partitions...).Values()
}

func sortTopicPartitions(partitions []TopicPartition) []TopicPartition {
	return collectionlist.NewList(partitions...).
		Sort(func(left, right TopicPartition) int {
			if left.Topic == right.Topic {
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).Values()
}
