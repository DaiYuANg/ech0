package broker

import (
	"time"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/arcgolabs/dix"
	"github.com/gofiber/fiber/v3"
	"github.com/samber/oops"
)

type healthOutput struct {
	Body RuntimeHealth `json:"body"`
}

type topicsOutput struct {
	Body struct {
		Topics []TopicSummary `json:"topics"`
	} `json:"body"`
}

type metricsOutput struct {
	Body struct {
		Snapshot MetricsSnapshot `json:"snapshot"`
		Runtime  string          `json:"runtime"`
	} `json:"body"`
}

type quotaOutput struct {
	Body struct {
		Quota QuotaSummary `json:"quota"`
	} `json:"body"`
}

type clusterOutput struct {
	Body struct {
		Cluster ClusterMetadata `json:"cluster"`
	} `json:"body"`
}

type runtimeEventsOutput struct {
	Body struct {
		Events       []runtimeEventSummary `json:"events"`
		BrokerEvents []BrokerEventSummary  `json:"broker_events,omitempty"`
	} `json:"body"`
}

type runtimeEventsStreamEvent struct {
	RuntimeEvents []runtimeEventSummary `json:"runtime_events"`
	BrokerEvents  []BrokerEventSummary  `json:"broker_events,omitempty"`
}

type runtimeEventSummary struct {
	At     time.Time                              `json:"at"`
	Kind   string                                 `json:"kind"`
	Fields *collectionmapping.Map[string, string] `json:"fields,omitempty"`
}

func (s *AdminServer) apiDocs(c fiber.Ctx) error {
	c.Type("html")
	return wrapBroker("admin_docs_failed", c.SendString(adminDocsHTML()), "write admin docs")
}

func (s *AdminServer) apiOpenAPI(c fiber.Ctx) error {
	return adminJSON(c, adminOpenAPISpec())
}

func (s *AdminServer) apiHealth(c fiber.Ctx) error {
	out := &healthOutput{}
	out.Body = s.broker.RuntimeHealth()
	return adminJSON(c, out.Body)
}

func (s *AdminServer) apiTopics(c fiber.Ctx) error {
	topics, err := s.broker.TopicSummariesFor(c.Context())
	if err != nil {
		return adminJSONError(c, wrapBroker("list_topics_failed", err, "list topics"))
	}
	out := &topicsOutput{}
	out.Body.Topics = topics
	return adminJSON(c, out.Body)
}

func (s *AdminServer) apiMetrics(c fiber.Ctx) error {
	if err := s.metrics.RefreshStream(c.Context(), s.broker); err != nil {
		return adminJSONError(c, oops.In("admin").Code("list_topics_failed").Wrapf(err, "build metrics"))
	}
	out := &metricsOutput{}
	out.Body.Snapshot = s.metrics.Snapshot()
	out.Body.Runtime = s.broker.RuntimeHealth().RuntimeMode
	return adminJSON(c, out.Body)
}

func (s *AdminServer) apiQuota(c fiber.Ctx) error {
	quota, err := s.broker.QuotaSummaryFor(c.Context())
	if err != nil {
		return adminJSONError(c, oops.In("admin").Code("quota_summary_failed").Wrapf(err, "build quota summary"))
	}
	out := &quotaOutput{}
	out.Body.Quota = quota
	return adminJSON(c, out.Body)
}

func (s *AdminServer) apiCluster(c fiber.Ctx) error {
	out := &clusterOutput{}
	out.Body.Cluster = s.broker.ClusterMetadata()
	return adminJSON(c, out.Body)
}

func (s *AdminServer) apiRuntimeEvents(c fiber.Ctx) error {
	out := &runtimeEventsOutput{}
	out.Body.Events = runtimeEventSummaries(s.events)
	out.Body.BrokerEvents = brokerEventSummaries(s.brokerEvents)
	return adminJSON(c, out.Body)
}

func (s *AdminServer) apiRuntimeEventsStream(c fiber.Ctx) error {
	c.Set(fiber.HeaderContentType, "text/event-stream")
	c.Set(fiber.HeaderCacheControl, "no-cache")
	body, err := runtimeEventSSE("runtime_events", s.runtimeEventStreamSnapshot())
	if err != nil {
		return adminJSONError(c, err)
	}
	return wrapBroker("admin_runtime_events_stream_failed", c.SendString(body), "write runtime events stream")
}

func (s *AdminServer) runtimeEventStreamSnapshot() runtimeEventsStreamEvent {
	return runtimeEventsStreamEvent{
		RuntimeEvents: runtimeEventSummaries(s.events),
		BrokerEvents:  brokerEventSummaries(s.brokerEvents),
	}
}

func runtimeEventSummaries(recorder *dix.EventRecorder) []runtimeEventSummary {
	if recorder == nil {
		return []runtimeEventSummary{}
	}
	records := recorder.Events()
	events := make([]runtimeEventSummary, 0, records.Len())
	records.ViewValues(func(items []dix.EventRecord) {
		for _, record := range items {
			events = append(events, runtimeEventSummary{
				At:     record.At,
				Kind:   eventKind(record.Event),
				Fields: eventFieldsMap(eventFields(record.Event)),
			})
		}
	})
	return events
}

func brokerEventSummaries(recorder *BrokerEventRecorder) []BrokerEventSummary {
	if recorder == nil {
		return []BrokerEventSummary{}
	}
	return recorder.Events()
}

func runtimeEventSSE(event string, data any) (string, error) {
	raw, err := marshalJSON(data)
	if err != nil {
		return "", wrapBroker("admin_sse_json_failed", err, "marshal runtime events stream")
	}
	return "event: " + event + "\n" + "data: " + string(raw) + "\n\n", nil
}

func adminDocsHTML() string {
	return `<!doctype html>
<html lang="en">
<head><meta charset="utf-8"><title>ech0 Admin API</title></head>
<body>
<h1>ech0 Admin API</h1>
<p>Native Fiber v3 admin endpoints are available under <code>/api</code>.</p>
<p><a href="/openapi.json">OpenAPI JSON</a></p>
</body>
</html>`
}

func adminOpenAPISpec() map[string]any {
	return map[string]any{
		"openapi": "3.1.0",
		"info": map[string]any{
			"title":       "ech0 Admin API",
			"version":     "0.1.0",
			"description": "Operational API for ech0 broker nodes.",
		},
		"paths": map[string]any{
			"/api/healthz": map[string]any{"get": map[string]any{"summary": "Broker health"}},
			"/api/topics":  map[string]any{"get": map[string]any{"summary": "Topic summaries"}},
			"/api/metrics": map[string]any{"get": map[string]any{"summary": "Metrics snapshot"}},
			"/api/quota":   map[string]any{"get": map[string]any{"summary": "Quota summary"}},
			"/api/cluster": map[string]any{"get": map[string]any{"summary": "Cluster metadata"}},
			"/api/gateway/topics/{topic}/records": map[string]any{
				"post": map[string]any{"summary": "Produce one record through the admin gateway"},
			},
			"/api/gateway/topics/{topic}/partitions/{partition}/records": map[string]any{
				"get": map[string]any{"summary": "Fetch records through the admin gateway"},
			},
			"/api/gateway/topics/{topic}/partitions/{partition}/commit": map[string]any{
				"post": map[string]any{"summary": "Commit an offset through the admin gateway"},
			},
		},
	}
}
