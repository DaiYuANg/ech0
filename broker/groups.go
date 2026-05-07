package broker

import (
	"cmp"
	"context"
	"strconv"

	"github.com/DaiYuANg/ech0/store"
	collectionlist "github.com/arcgolabs/collectionx/list"
	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	collectionset "github.com/arcgolabs/collectionx/set"
)

type GroupAssignmentStrategy string

const (
	GroupAssignmentRoundRobin GroupAssignmentStrategy = "round_robin"
	GroupAssignmentRange      GroupAssignmentStrategy = "range"
)

type groupRebalancePlan struct {
	Assignment       store.ConsumerGroupAssignment
	Strategy         GroupAssignmentStrategy
	ActiveMembers    int
	MovedPartitions  uint64
	StickyCandidates uint64
	StickyApplied    uint64
	MemberLoads      []GroupMemberLoadSummary
}

func (b *Broker) GroupRebalanceExplain(group string) (GroupRebalanceExplainSummary, error) {
	now := store.NowMS()
	members, err := b.activeGroupMembers(group, now)
	if err != nil {
		return GroupRebalanceExplainSummary{}, err
	}
	previous, err := b.meta.LoadGroupAssignment(group)
	if err != nil {
		return GroupRebalanceExplainSummary{}, wrapBrokerStore(err, "load group assignment")
	}
	plan, err := b.buildGroupRebalancePlan(group, now, previous, members)
	if err != nil {
		return GroupRebalanceExplainSummary{}, err
	}
	return GroupRebalanceExplainSummary{
		Group:             group,
		NextGeneration:    plan.Assignment.Generation,
		Strategy:          string(plan.Strategy),
		StickyAssignments: b.cfg.Broker.GroupStickyAssignments,
		ActiveMembers:     plan.ActiveMembers,
		TotalAssignments:  len(plan.Assignment.Assignments),
		MovedPartitions:   plan.MovedPartitions,
		StickyCandidates:  plan.StickyCandidates,
		StickyApplied:     plan.StickyApplied,
		MemberLoads:       plan.MemberLoads,
	}, nil
}

func (b *Broker) activeGroupMembers(group string, now uint64) ([]store.ConsumerGroupMember, error) {
	members, err := b.meta.ListGroupMembers(group)
	if err != nil {
		return nil, wrapBrokerStore(err, "list group members")
	}
	return collectionlist.FilterList(
		collectionlist.NewList(members...),
		func(_ int, member store.ConsumerGroupMember) bool {
			return !member.ExpiredAt(now)
		},
	).Sort(func(left, right store.ConsumerGroupMember) int {
		return cmp.Compare(left.MemberID, right.MemberID)
	}).Values(), nil
}

func (b *Broker) buildGroupRebalancePlan(group string, now uint64, previous *store.ConsumerGroupAssignment, active []store.ConsumerGroupMember) (groupRebalancePlan, error) {
	strategy := normalizeGroupAssignmentStrategy(b.cfg.Broker.GroupAssignmentStrategy)
	generation := uint64(1)
	if previous != nil {
		generation = previous.Generation + 1
	}
	assignments := collectionlist.NewList[store.GroupPartitionAssignment]()
	if len(active) > 0 {
		partitions, err := b.groupPartitions(active)
		if err != nil {
			return groupRebalancePlan{}, err
		}
		switch strategy {
		case GroupAssignmentRoundRobin:
			assignments = buildRoundRobinAssignments(active, partitions)
		case GroupAssignmentRange:
			assignments = buildRangeAssignments(active, partitions)
		}
	}
	stickyCandidates, stickyApplied := uint64(0), uint64(0)
	if b.cfg.Broker.GroupStickyAssignments && previous != nil {
		stickyCandidates, stickyApplied = applyStickyAssignments(assignments, previous.Assignments, active)
	}
	assignment := store.ConsumerGroupAssignment{
		Group:       group,
		Generation:  generation,
		Assignments: sortGroupAssignments(assignments.Values()),
		UpdatedAtMS: now,
	}
	return groupRebalancePlan{
		Assignment:       assignment,
		Strategy:         strategy,
		ActiveMembers:    len(active),
		MovedPartitions:  movedPartitions(previous, assignment.Assignments),
		StickyCandidates: stickyCandidates,
		StickyApplied:    stickyApplied,
		MemberLoads:      memberLoads(assignment.Assignments),
	}, nil
}

func buildRoundRobinAssignments(active []store.ConsumerGroupMember, partitions []store.TopicPartition) *collectionlist.List[store.GroupPartitionAssignment] {
	assignments := collectionlist.NewList[store.GroupPartitionAssignment]()
	for i, tp := range partitions {
		owner := active[i%len(active)]
		assignments.Add(store.GroupPartitionAssignment{
			MemberID:  owner.MemberID,
			Topic:     tp.Topic,
			Partition: tp.Partition,
		})
	}
	return assignments
}

func buildRangeAssignments(active []store.ConsumerGroupMember, partitions []store.TopicPartition) *collectionlist.List[store.GroupPartitionAssignment] {
	byTopic := partitionsByTopic(partitions)
	assignments := collectionlist.NewList[store.GroupPartitionAssignment]()
	topics := collectionlist.NewList(byTopic.Keys()...).Sort(cmp.Compare).Values()
	for _, topic := range topics {
		appendRangeAssignments(assignments, eligibleMembers(active, topic), sortedTopicPartitions(byTopic.GetOrDefault(topic, nil)))
	}
	return assignments
}

func partitionsByTopic(partitions []store.TopicPartition) *collectionmapping.Map[string, []store.TopicPartition] {
	byTopic := collectionmapping.NewMap[string, []store.TopicPartition]()
	for _, tp := range partitions {
		items := byTopic.GetOrDefault(tp.Topic, nil)
		byTopic.Set(tp.Topic, append(items, tp))
	}
	return byTopic
}

func sortedTopicPartitions(partitions []store.TopicPartition) []store.TopicPartition {
	return collectionlist.NewList(partitions...).
		Sort(func(left, right store.TopicPartition) int {
			return cmp.Compare(left.Partition, right.Partition)
		}).Values()
}

func eligibleMembers(active []store.ConsumerGroupMember, topic string) []store.ConsumerGroupMember {
	eligible := collectionlist.NewList[store.ConsumerGroupMember]()
	for _, member := range active {
		if memberWantsTopic(member, topic) {
			eligible.Add(member)
		}
	}
	return eligible.Values()
}

func appendRangeAssignments(assignments *collectionlist.List[store.GroupPartitionAssignment], owners []store.ConsumerGroupMember, partitions []store.TopicPartition) {
	if len(owners) == 0 {
		return
	}
	base := len(partitions) / len(owners)
	extra := len(partitions) % len(owners)
	cursor := 0
	for i, owner := range owners {
		size := base
		if i < extra {
			size++
		}
		for _, tp := range partitions[cursor : cursor+size] {
			assignments.Add(store.GroupPartitionAssignment{
				MemberID:  owner.MemberID,
				Topic:     tp.Topic,
				Partition: tp.Partition,
			})
		}
		cursor += size
	}
}

func applyStickyAssignments(assignments *collectionlist.List[store.GroupPartitionAssignment], previous []store.GroupPartitionAssignment, active []store.ConsumerGroupMember) (uint64, uint64) {
	previousOwners := collectionmapping.NewMap[string, string]()
	for _, item := range previous {
		previousOwners.Set(groupAssignmentKey(item.Topic, item.Partition), item.MemberID)
	}
	activeMembers := collectionmapping.NewMap[string, store.ConsumerGroupMember]()
	for _, member := range active {
		activeMembers.Set(member.MemberID, member)
	}
	stickyCandidates, stickyApplied := uint64(0), uint64(0)
	assignments.SetAllIndexed(func(_ int, item store.GroupPartitionAssignment) store.GroupPartitionAssignment {
		previousOwner, ok := previousOwners.Get(groupAssignmentKey(item.Topic, item.Partition))
		if !ok || previousOwner == item.MemberID {
			return item
		}
		member, activeOK := activeMembers.Get(previousOwner)
		if !activeOK || !memberWantsTopic(member, item.Topic) {
			return item
		}
		stickyCandidates++
		item.MemberID = previousOwner
		stickyApplied++
		return item
	})
	return stickyCandidates, stickyApplied
}

func movedPartitions(previous *store.ConsumerGroupAssignment, assignments []store.GroupPartitionAssignment) uint64 {
	if previous == nil {
		return 0
	}
	previousOwners := collectionmapping.NewMap[string, string]()
	for _, item := range previous.Assignments {
		previousOwners.Set(groupAssignmentKey(item.Topic, item.Partition), item.MemberID)
	}
	moved := uint64(0)
	for _, item := range assignments {
		previousOwner, ok := previousOwners.Get(groupAssignmentKey(item.Topic, item.Partition))
		if ok && previousOwner != item.MemberID {
			moved++
		}
	}
	return moved
}

func memberLoads(assignments []store.GroupPartitionAssignment) []GroupMemberLoadSummary {
	loads := collectionmapping.NewMap[string, int]()
	for _, assignment := range assignments {
		loads.Set(assignment.MemberID, loads.GetOrDefault(assignment.MemberID, 0)+1)
	}
	members := collectionlist.NewList(loads.Keys()...).
		Sort(cmp.Compare)
	return collectionlist.MapList(members, func(_ int, memberID string) GroupMemberLoadSummary {
		return GroupMemberLoadSummary{MemberID: memberID, Partitions: loads.GetOrDefault(memberID, 0)}
	}).Values()
}

func sortGroupAssignments(assignments []store.GroupPartitionAssignment) []store.GroupPartitionAssignment {
	return collectionlist.NewList(assignments...).
		Sort(func(left, right store.GroupPartitionAssignment) int {
			if left.Topic == right.Topic {
				if left.Partition == right.Partition {
					return cmp.Compare(left.MemberID, right.MemberID)
				}
				return cmp.Compare(left.Partition, right.Partition)
			}
			return cmp.Compare(left.Topic, right.Topic)
		}).Values()
}

func memberWantsTopic(member store.ConsumerGroupMember, topic string) bool {
	return collectionset.NewSet(member.Topics...).Contains(topic)
}

func normalizeGroupAssignmentStrategy(raw string) GroupAssignmentStrategy {
	switch GroupAssignmentStrategy(raw) {
	case GroupAssignmentRoundRobin:
		return GroupAssignmentRoundRobin
	case GroupAssignmentRange:
		return GroupAssignmentRange
	default:
		return GroupAssignmentRoundRobin
	}
}

func groupAssignmentKey(topic string, partition uint32) string {
	return topic + "\x00" + strconvUint32(partition)
}

func strconvUint32(value uint32) string {
	return strconv.FormatUint(uint64(value), 10)
}

func recordRebalanceMetrics(ctx context.Context, metrics *MetricsRuntime, plan groupRebalancePlan) {
	if metrics != nil {
		metrics.RecordRebalance(ctx, string(plan.Strategy), uint64(len(plan.Assignment.Assignments)), plan.MovedPartitions)
	}
}
