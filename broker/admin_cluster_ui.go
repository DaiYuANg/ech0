package broker

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/lyonbrown4d/ech0/store"
)

type clusterView struct {
	Cluster     ClusterMetadata
	Topics      []TopicSummary
	Result      string
	Error       string
	TopicsError string
}

func (s *AdminServer) uiCluster(c *fiber.Ctx) error {
	topics, err := s.broker.TopicSummariesFor(c.UserContext())
	view := clusterView{
		Cluster: s.broker.ClusterMetadata(),
		Topics:  topics,
		Result:  c.Query("result"),
		Error:   c.Query("error"),
	}
	if err != nil {
		view.TopicsError = err.Error()
	}
	return adminRender(c, "admin_templates/cluster", view)
}

func (s *AdminServer) uiClusterNodeJoin(c *fiber.Ctx) error {
	req, err := adminClusterNodePayload(c)
	if err != nil {
		return redirectClusterError(c, err)
	}
	result, err := s.broker.JoinClusterNode(c.UserContext(), req.NodeID, req.Addr)
	return redirectClusterResult(c, clusterMembershipMessage(result), err)
}

func (s *AdminServer) uiClusterNodeLeave(c *fiber.Ctx) error {
	req, err := adminClusterNodePayload(c)
	if err != nil {
		return redirectClusterError(c, err)
	}
	result, err := s.broker.LeaveClusterNode(c.UserContext(), req.NodeID)
	return redirectClusterResult(c, clusterMembershipMessage(result), err)
}

func (s *AdminServer) uiClusterLeadershipTransfer(c *fiber.Ctx) error {
	req, err := adminLeadershipTransferPayload(c)
	if err != nil {
		return redirectClusterError(c, err)
	}
	result, err := s.broker.TransferClusterLeadership(c.UserContext(), req.GroupID, req.TargetNode)
	return redirectClusterResult(c, leadershipTransferMessage(result), err)
}

func (s *AdminServer) uiClusterLeadershipBalance(c *fiber.Ctx) error {
	result, err := s.broker.BalanceClusterLeadership(c.UserContext())
	return redirectClusterResult(c, leadershipBalanceMessage(result), err)
}

func (s *AdminServer) uiClusterPartitionReassign(c *fiber.Ctx) error {
	req, err := adminPartitionReassignPayload(c)
	if err != nil {
		return redirectClusterError(c, err)
	}
	err = s.broker.ReassignPartition(c.UserContext(), req.Topic, req.Partition, store.ShardID(req.ShardID))
	return redirectClusterResult(c, "partition reassignment requested", err)
}

func clusterMembershipMessage(result ClusterMembershipResult) string {
	return result.Operation + " node " + strconv.FormatUint(result.NodeID, 10) + " changed " + changedGroups(result.Groups) + " groups"
}

func changedGroups(groups []ClusterMembershipGroupResult) string {
	changed := 0
	for _, group := range groups {
		if group.Changed {
			changed++
		}
	}
	return strconv.Itoa(changed) + "/" + strconv.Itoa(len(groups))
}

func leadershipTransferMessage(result ClusterLeadershipTransferResult) string {
	if !result.Requested {
		return "leadership transfer not requested"
	}
	return "leadership transfer requested for group " + strconv.FormatUint(result.GroupID, 10)
}

func leadershipBalanceMessage(result ClusterLeadershipBalanceResult) string {
	return "leadership balance requested " + strconv.Itoa(len(result.Transfers)) + " transfers"
}

func redirectClusterResult(c *fiber.Ctx, result string, err error) error {
	if err != nil {
		return redirectClusterError(c, err)
	}
	return redirectCluster(c, "result", result)
}

func redirectClusterError(c *fiber.Ctx, err error) error {
	return redirectCluster(c, "error", err.Error())
}

func redirectCluster(c *fiber.Ctx, key, value string) error {
	target := "/ui/cluster"
	if strings.TrimSpace(value) != "" {
		target += "?" + key + "=" + url.QueryEscape(value)
	}
	return wrapBroker("admin_cluster_redirect_failed", c.Redirect(target, fiber.StatusSeeOther), "redirect cluster ui")
}
