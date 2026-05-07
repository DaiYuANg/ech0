package broker

import (
	"net/http"
	"strconv"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/gofiber/fiber/v2"
	fiberhtml "github.com/gofiber/template/html/v2"
)

func adminTemplateEngine() *fiberhtml.Engine {
	engine := fiberhtml.NewFileSystem(http.FS(adminTemplates), ".html")
	engine.AddFunc("fmtOffset", displayUint64Ptr)
	engine.AddFunc("fmtUint", strconv.FormatUint)
	return engine
}

func (s *AdminServer) uiDashboard(c *fiber.Ctx) error {
	if err := s.metrics.RefreshStream(c.UserContext(), s.broker); err != nil && s.logger != nil {
		s.logger.Warn("refresh metrics failed", "error", err)
	}
	topics, err := s.broker.TopicSummaries()
	view := dashboardView{
		Health:  s.broker.RuntimeHealth(),
		Metrics: s.metrics.Snapshot(),
	}
	if err != nil {
		view.TopicsError = err.Error()
	} else {
		view.Topics = topics
	}
	view.CommandErrorRate = commandErrorRate(view.Metrics)
	return adminRender(c, "admin_templates/dashboard", view)
}

func commandErrorRate(metrics MetricsSnapshot) string {
	if metrics.CommandsTotal == 0 {
		return "0.00"
	}
	rate := float64(metrics.CommandErrorsTotal) / float64(metrics.CommandsTotal) * 100
	return strconv.FormatFloat(rate, 'f', 2, 64)
}

func (s *AdminServer) uiTopics(c *fiber.Ctx) error {
	topics, err := s.broker.TopicSummaries()
	view := topicsView{Topics: topics}
	if err != nil {
		view.Error = err.Error()
	}
	return adminRender(c, "admin_templates/topics", view)
}

func (s *AdminServer) uiTopicMessages(c *fiber.Ctx) error {
	topic := c.Params("topic")
	partition := parseUint32Query(c, "partition")
	offset := parseUint64Query(c, "offset")
	limit := c.QueryInt("limit", 50)
	page, err := s.broker.TopicMessagesSnapshot(topic, partition, offset, limit)
	view := topicMessagesView{
		Page:       page,
		PrevOffset: previousOffset(offset, limit),
	}
	if err != nil {
		view.Error = err.Error()
		view.Page = TopicMessagesPageSummary{Topic: topic, Partition: partition, Offset: offset, Limit: limit, NextOffset: offset}
	}
	return adminRender(c, "admin_templates/topic_messages", view)
}

func previousOffset(offset uint64, limit int) uint64 {
	if limit > 0 && offset > uint64(limit) {
		return offset - uint64(limit)
	}
	return 0
}

func (s *AdminServer) uiGroup(c *fiber.Ctx) error {
	group := c.Params("group")
	explain, explainErr := s.broker.GroupRebalanceExplain(group)
	members, membersErr := s.broker.GroupMembersSnapshot(group)
	assignment, assignmentErr := s.broker.GroupAssignmentSnapshot(group)
	lag, lagErr := s.broker.GroupLagSnapshot(group)
	view := groupView{
		Group:      group,
		Explain:    &explain,
		Members:    members,
		Assignment: assignment,
		Lag:        lag,
		Error:      strings.Join(collectionStrings(explainErr, membersErr, assignmentErr, lagErr), "; "),
	}
	if explainErr != nil {
		view.Explain = nil
	}
	return adminRender(c, "admin_templates/group", view)
}

func (s *AdminServer) apiGroupMembers(c *fiber.Ctx) error {
	members, err := s.broker.GroupMembersSnapshot(c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, members)
}

func (s *AdminServer) apiGroupAssignment(c *fiber.Ctx) error {
	assignment, err := s.broker.GroupAssignmentSnapshot(c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, assignment)
}

func (s *AdminServer) apiGroupLag(c *fiber.Ctx) error {
	lag, err := s.broker.GroupLagSnapshot(c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, lag)
}

func (s *AdminServer) apiGroupRebalance(c *fiber.Ctx) error {
	assignment, err := s.broker.RebalanceConsumerGroup(c.UserContext(), c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, groupAssignmentSummary(assignment))
}

func (s *AdminServer) apiGroupRebalanceExplain(c *fiber.Ctx) error {
	explain, err := s.broker.GroupRebalanceExplain(c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, explain)
}

func adminJSONError(c *fiber.Ctx, err error) error {
	response := adminErrorResponse{Error: err.Error()}
	return wrapBroker("admin_json_error_failed", c.Status(fiber.StatusInternalServerError).JSON(response), "write admin json error")
}

func adminRender(c *fiber.Ctx, name string, view any) error {
	return wrapBroker("admin_render_failed", c.Render(name, view), "render admin template %s", name)
}

func adminJSON(c *fiber.Ctx, value any) error {
	return wrapBroker("admin_json_failed", c.JSON(value), "write admin json")
}

func collectionStrings(errs ...error) []string {
	out := collectionlist.NewList[string]()
	for _, err := range errs {
		if err != nil {
			out.Add(err.Error())
		}
	}
	return out.Values()
}

type adminErrorResponse struct {
	Error string `json:"error"`
}

func parseUint32Query(c *fiber.Ctx, key string) uint32 {
	value, err := strconv.ParseUint(c.Query(key, "0"), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(value)
}

func parseUint64Query(c *fiber.Ctx, key string) uint64 {
	value, err := strconv.ParseUint(c.Query(key, "0"), 10, 64)
	if err != nil {
		return 0
	}
	return value
}
