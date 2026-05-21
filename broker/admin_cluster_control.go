package broker

import (
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/lyonbrown4d/ech0/store"
)

type adminClusterNodeRequest struct {
	NodeID uint64 `json:"node_id"`
	Addr   string `json:"addr"`
}

type adminLeadershipTransferRequest struct {
	GroupID    uint64 `json:"group_id"`
	TargetNode uint64 `json:"target_node"`
}

type adminPartitionReassignRequest struct {
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
	ShardID   uint32 `json:"shard_id"`
}

func (s *AdminServer) registerClusterControlRoutes() {
	s.app.Post("/api/cluster/nodes/join", s.apiClusterNodeJoin)
	s.app.Post("/api/cluster/nodes/leave", s.apiClusterNodeLeave)
	s.app.Post("/api/cluster/leadership/transfer", s.apiClusterLeadershipTransfer)
	s.app.Post("/api/cluster/leadership/balance", s.apiClusterLeadershipBalance)
	s.app.Post("/api/cluster/partitions/reassign", s.apiClusterPartitionReassign)
}

func (s *AdminServer) apiClusterNodeJoin(c *fiber.Ctx) error {
	req, err := adminClusterNodePayload(c)
	if err != nil {
		return adminJSONError(c, err)
	}
	result, err := s.broker.JoinClusterNode(c.UserContext(), req.NodeID, req.Addr)
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, result)
}

func (s *AdminServer) apiClusterNodeLeave(c *fiber.Ctx) error {
	req, err := adminClusterNodePayload(c)
	if err != nil {
		return adminJSONError(c, err)
	}
	result, err := s.broker.LeaveClusterNode(c.UserContext(), req.NodeID)
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, result)
}

func (s *AdminServer) apiClusterLeadershipTransfer(c *fiber.Ctx) error {
	req, err := adminLeadershipTransferPayload(c)
	if err != nil {
		return adminJSONError(c, err)
	}
	result, err := s.broker.TransferClusterLeadership(c.UserContext(), req.GroupID, req.TargetNode)
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, result)
}

func (s *AdminServer) apiClusterLeadershipBalance(c *fiber.Ctx) error {
	result, err := s.broker.BalanceClusterLeadership(c.UserContext())
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, result)
}

func (s *AdminServer) apiClusterPartitionReassign(c *fiber.Ctx) error {
	req, err := adminPartitionReassignPayload(c)
	if err != nil {
		return adminJSONError(c, err)
	}
	err = s.broker.ReassignPartition(c.UserContext(), req.Topic, req.Partition, store.ShardID(req.ShardID))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, map[string]any{"reassigned": true})
}

func adminClusterNodePayload(c *fiber.Ctx) (adminClusterNodeRequest, error) {
	var req adminClusterNodeRequest
	if err := parseAdminPayload(c, &req); err != nil {
		return adminClusterNodeRequest{}, err
	}
	req.NodeID = uint64FormValue(c, "node_id", req.NodeID)
	req.Addr = stringFormValue(c, "addr", req.Addr)
	return req, nil
}

func adminLeadershipTransferPayload(c *fiber.Ctx) (adminLeadershipTransferRequest, error) {
	var req adminLeadershipTransferRequest
	if err := parseAdminPayload(c, &req); err != nil {
		return adminLeadershipTransferRequest{}, err
	}
	req.GroupID = uint64FormValue(c, "group_id", req.GroupID)
	req.TargetNode = uint64FormValue(c, "target_node", req.TargetNode)
	return req, nil
}

func adminPartitionReassignPayload(c *fiber.Ctx) (adminPartitionReassignRequest, error) {
	var req adminPartitionReassignRequest
	if err := parseAdminPayload(c, &req); err != nil {
		return adminPartitionReassignRequest{}, err
	}
	req.Topic = stringFormValue(c, "topic", req.Topic)
	req.Partition = uint32FormValue(c, "partition", req.Partition)
	req.ShardID = uint32FormValue(c, "shard_id", req.ShardID)
	return req, nil
}

func parseAdminPayload(c *fiber.Ctx, out any) error {
	if len(c.Body()) == 0 || !strings.Contains(strings.ToLower(c.Get(fiber.HeaderContentType)), "json") {
		return nil
	}
	return wrapBroker("admin_json_parse_failed", unmarshalJSON(c.Body(), out), "parse admin json request")
}

func stringFormValue(c *fiber.Ctx, key, fallback string) string {
	value := strings.TrimSpace(c.FormValue(key))
	if value == "" {
		return strings.TrimSpace(fallback)
	}
	return value
}

func uint64FormValue(c *fiber.Ctx, key string, fallback uint64) uint64 {
	value := strings.TrimSpace(c.FormValue(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func uint32FormValue(c *fiber.Ctx, key string, fallback uint32) uint32 {
	value := strings.TrimSpace(c.FormValue(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return fallback
	}
	return uint32(parsed)
}
