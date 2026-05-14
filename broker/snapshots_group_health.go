package broker

import (
	"cmp"
	"context"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/lyonbrown4d/ech0/store"
)

func (b *Broker) GroupHealthSnapshot(group string) (*GroupHealthSummary, error) {
	return b.GroupHealthSnapshotFor(context.Background(), group)
}

func (b *Broker) GroupHealthSnapshotFor(ctx context.Context, group string) (*GroupHealthSummary, error) {
	members, err := b.GroupMembersSnapshotFor(ctx, group)
	if err != nil {
		return nil, err
	}
	assignment, err := b.GroupAssignmentSnapshotFor(ctx, group)
	if err != nil {
		return nil, err
	}
	lag, err := b.GroupLagSnapshotFor(ctx, group)
	if err != nil {
		return nil, err
	}
	explain, err := b.GroupRebalanceExplainFor(ctx, group)
	if err != nil {
		return nil, err
	}
	history, err := b.GroupRebalanceHistorySnapshotFor(ctx, group, 20)
	if err != nil {
		return nil, err
	}
	summary := groupHealthSummary(group, members, assignment, lag, &explain, history)
	return &summary, nil
}

func (b *Broker) GroupRebalanceHistorySnapshot(group string, limit int) ([]GroupRebalanceHistorySummary, error) {
	return b.GroupRebalanceHistorySnapshotFor(context.Background(), group, limit)
}

func (b *Broker) GroupRebalanceHistorySnapshotFor(ctx context.Context, group string, limit int) ([]GroupRebalanceHistorySummary, error) {
	identity := b.identity(ctx)
	if err := b.authorize(ctx, identity, ACLActionDescribe, groupResource(identity, group)); err != nil {
		return nil, err
	}
	scopedGroup := scopedName(identity, "group", group)
	values := []GroupRebalanceHistorySummary{}
	if b.groupRebalanceHistory != nil {
		values = b.groupRebalanceHistory.Values()
	}
	out := collectionlist.NewList[GroupRebalanceHistorySummary]()
	for _, item := range values {
		if item.Group == scopedGroup {
			out.Add(visibleGroupRebalanceHistorySummary(identity, item))
		}
	}
	items := out.Sort(compareGroupRebalanceHistory).Values()
	if limit > 0 && len(items) > limit {
		items = items[len(items)-limit:]
	}
	return items, nil
}

func (b *Broker) recordGroupRebalanceHistory(plan groupRebalancePlan) {
	if b.groupRebalanceHistory == nil {
		return
	}
	b.groupRebalanceHistory.Push(GroupRebalanceHistorySummary{
		Group:            plan.Assignment.Group,
		Generation:       plan.Assignment.Generation,
		Strategy:         string(plan.Strategy),
		ActiveMembers:    plan.ActiveMembers,
		TotalAssignments: len(plan.Assignment.Assignments),
		MovedPartitions:  plan.MovedPartitions,
		StickyCandidates: plan.StickyCandidates,
		StickyApplied:    plan.StickyApplied,
		UpdatedAtMS:      plan.Assignment.UpdatedAtMS,
		MemberLoads:      collectionlist.NewList(plan.MemberLoads...).Values(),
	})
}

func groupHealthSummary(
	group string,
	members []GroupMemberSummary,
	assignment *GroupAssignmentSummary,
	lag *GroupLagSummary,
	explain *GroupRebalanceExplainSummary,
	history []GroupRebalanceHistorySummary,
) GroupHealthSummary {
	summary := GroupHealthSummary{
		Group:            group,
		TotalMembers:     len(members),
		Members:          collectionlist.NewList(members...).Values(),
		Assignment:       assignment,
		Lag:              lag,
		RebalanceExplain: explain,
		RebalanceHistory: collectionlist.NewList(history...).Values(),
	}
	accumulateGroupHealthMembers(&summary, members)
	accumulateGroupHealthAssignment(&summary, assignment)
	accumulateGroupHealthLag(&summary, lag)
	summary.Status = groupHealthStatus(summary)
	return summary
}

func accumulateGroupHealthMembers(summary *GroupHealthSummary, members []GroupMemberSummary) {
	now := store.NowMS()
	for _, member := range members {
		if member.ExpiresAtMS <= now {
			summary.ExpiredMembers++
			continue
		}
		summary.ActiveMembers++
	}
}

func accumulateGroupHealthAssignment(summary *GroupHealthSummary, assignment *GroupAssignmentSummary) {
	if assignment == nil {
		return
	}
	summary.Generation = assignment.Generation
	summary.AssignedPartitions = len(assignment.Assignments)
}

func accumulateGroupHealthLag(summary *GroupHealthSummary, lag *GroupLagSummary) {
	if lag == nil {
		return
	}
	summary.TotalBacklogRecords = lag.TotalBacklogRecords
	summary.TotalLagRecords = lag.TotalLagRecords
	for _, partition := range lag.Partitions {
		summary.MaxPartitionLagRecords = max(summary.MaxPartitionLagRecords, partition.LagRecords)
	}
}

func groupHealthStatus(summary GroupHealthSummary) string {
	if summary.TotalMembers == 0 && summary.AssignedPartitions == 0 {
		return "empty"
	}
	if summary.ExpiredMembers > 0 {
		return "degraded"
	}
	if summary.TotalMembers > 0 && summary.AssignedPartitions == 0 {
		return "no_assignment"
	}
	if summary.TotalLagRecords > 0 {
		return "lagging"
	}
	return "healthy"
}

func compareGroupRebalanceHistory(left, right GroupRebalanceHistorySummary) int {
	if left.UpdatedAtMS != right.UpdatedAtMS {
		return cmp.Compare(left.UpdatedAtMS, right.UpdatedAtMS)
	}
	return cmp.Compare(left.Generation, right.Generation)
}

func visibleGroupRebalanceHistorySummary(identity Identity, summary GroupRebalanceHistorySummary) GroupRebalanceHistorySummary {
	summary.Group = visibleName(identity, "group", summary.Group)
	return summary
}
