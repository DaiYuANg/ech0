package broker

import (
	"net/url"
	"strconv"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/gofiber/fiber/v2"
)

type operationsView struct {
	Cluster          ClusterMetadata
	Result           string
	Error            string
	WebhookSinks     []operationSinkView
	FileSinks        []operationSinkView
	MirrorSinks      []operationSinkView
	S3Sinks          []operationSinkView
	DatabaseOutboxes []operationSinkView
}

type operationSinkView struct {
	Name      string
	Topic     string
	Partition uint32
	Target    string
	Action    string
}

func (s *AdminServer) uiOperations(c *fiber.Ctx) error {
	return adminRender(c, "admin_templates/ops", operationsView{
		Cluster:          s.broker.ClusterMetadata(),
		Result:           c.Query("result"),
		Error:            c.Query("error"),
		WebhookSinks:     webhookSinkViews(s.cfg.Broker.WebhookSinks),
		FileSinks:        fileSinkViews(s.cfg.Broker.FileSinks),
		MirrorSinks:      mirrorSinkViews(s.cfg.Broker.MirrorSinks),
		S3Sinks:          s3SinkViews(s.cfg.Broker.S3Sinks),
		DatabaseOutboxes: databaseOutboxViews(s.cfg.Broker.DatabaseOutboxes),
	})
}

func (s *AdminServer) apiRunWebhookSink(c *fiber.Ctx) error {
	sink, ok := findWebhookSink(s.cfg.Broker.WebhookSinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "webhook sink not found")
	}
	result, err := s.broker.ProcessWebhookSinkOnce(c.UserContext(), sink)
	return redirectOpsResult(c, "webhook delivered "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunFileSink(c *fiber.Ctx) error {
	sink, ok := findFileSink(s.cfg.Broker.FileSinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "file sink not found")
	}
	result, err := s.broker.ProcessFileSinkOnce(c.UserContext(), sink)
	return redirectOpsResult(c, "file wrote "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunMirrorSink(c *fiber.Ctx) error {
	sink, ok := findMirrorSink(s.cfg.Broker.MirrorSinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "mirror sink not found")
	}
	result, err := s.broker.ProcessMirrorSinkOnce(c.UserContext(), sink)
	return redirectOpsResult(c, "mirror replicated "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunS3Sink(c *fiber.Ctx) error {
	sink, ok := findS3Sink(s.cfg.Broker.S3Sinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "s3 sink not found")
	}
	result, err := s.broker.ProcessS3SinkOnce(c.UserContext(), sink)
	return redirectOpsResult(c, "s3 wrote "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunDatabaseOutbox(c *fiber.Ctx) error {
	outbox, ok := findDatabaseOutbox(s.cfg.Broker.DatabaseOutboxes, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "database outbox not found")
	}
	result, err := s.broker.ProcessDatabaseOutboxOnce(c.UserContext(), outbox)
	return redirectOpsResult(c, "outbox published "+strconv.Itoa(result.Published), err)
}

func webhookSinkViews(sinks []WebhookSinkConfig) []operationSinkView {
	out := collectionlist.NewListWithCapacity[operationSinkView](len(sinks))
	for index := range sinks {
		sink := sinks[index]
		out.Add(operationSinkView{Name: webhookSinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: sink.URL, Action: "/api/ops/webhook-sinks/run"})
	}
	return out.Values()
}

func fileSinkViews(sinks []FileSinkConfig) []operationSinkView {
	out := collectionlist.NewListWithCapacity[operationSinkView](len(sinks))
	for index := range sinks {
		sink := sinks[index]
		out.Add(operationSinkView{Name: fileSinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: sink.Path, Action: "/api/ops/file-sinks/run"})
	}
	return out.Values()
}

func mirrorSinkViews(sinks []MirrorSinkConfig) []operationSinkView {
	out := collectionlist.NewListWithCapacity[operationSinkView](len(sinks))
	for index := range sinks {
		sink := sinks[index]
		out.Add(operationSinkView{Name: mirrorSinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: mirrorSinkAdminURL(sink), Action: "/api/ops/mirror-sinks/run"})
	}
	return out.Values()
}

func s3SinkViews(sinks []S3SinkConfig) []operationSinkView {
	out := collectionlist.NewListWithCapacity[operationSinkView](len(sinks))
	for index := range sinks {
		sink := sinks[index]
		out.Add(operationSinkView{Name: s3SinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: strings.TrimSpace(sink.Bucket), Action: "/api/ops/s3-sinks/run"})
	}
	return out.Values()
}

func databaseOutboxViews(outboxes []DatabaseOutboxConfig) []operationSinkView {
	out := collectionlist.NewListWithCapacity[operationSinkView](len(outboxes))
	for index := range outboxes {
		outbox := outboxes[index]
		out.Add(operationSinkView{Name: databaseOutboxName(outbox), Topic: outbox.Topic, Target: strings.TrimSpace(outbox.DriverName), Action: "/api/ops/database-outboxes/run"})
	}
	return out.Values()
}

func findWebhookSink(sinks []WebhookSinkConfig, name string) (WebhookSinkConfig, bool) {
	for index := range sinks {
		if webhookSinkName(sinks[index]) == strings.TrimSpace(name) {
			return sinks[index], true
		}
	}
	return WebhookSinkConfig{}, false
}

func findFileSink(sinks []FileSinkConfig, name string) (FileSinkConfig, bool) {
	for index := range sinks {
		if fileSinkName(sinks[index]) == strings.TrimSpace(name) {
			return sinks[index], true
		}
	}
	return FileSinkConfig{}, false
}

func findMirrorSink(sinks []MirrorSinkConfig, name string) (MirrorSinkConfig, bool) {
	for index := range sinks {
		if mirrorSinkName(sinks[index]) == strings.TrimSpace(name) {
			return sinks[index], true
		}
	}
	return MirrorSinkConfig{}, false
}

func findS3Sink(sinks []S3SinkConfig, name string) (S3SinkConfig, bool) {
	for index := range sinks {
		if s3SinkName(sinks[index]) == strings.TrimSpace(name) {
			return sinks[index], true
		}
	}
	return S3SinkConfig{}, false
}

func findDatabaseOutbox(outboxes []DatabaseOutboxConfig, name string) (DatabaseOutboxConfig, bool) {
	for index := range outboxes {
		if databaseOutboxName(outboxes[index]) == strings.TrimSpace(name) {
			return outboxes[index], true
		}
	}
	return DatabaseOutboxConfig{}, false
}

func redirectOpsResult(c *fiber.Ctx, result string, err error) error {
	if err != nil {
		return redirectOpsError(c, err.Error())
	}
	return redirectOps(c, "result", result)
}

func redirectOpsError(c *fiber.Ctx, message string) error {
	return redirectOps(c, "error", message)
}

func redirectOps(c *fiber.Ctx, key, value string) error {
	target := "/ui/ops"
	if strings.TrimSpace(value) != "" {
		target += "?" + key + "=" + url.QueryEscape(value)
	}
	return wrapBroker("admin_ops_redirect_failed", c.Redirect(target, fiber.StatusSeeOther), "redirect operations")
}
