package broker

import (
	"github.com/arcgolabs/httpx"
	"github.com/arcgolabs/httpx/adapter"
	httpxfiber "github.com/arcgolabs/httpx/adapter/fiber"
	"github.com/felixge/fgprof"
	fiberadaptor "github.com/gofiber/fiber/v2/middleware/adaptor"
)

func (s *AdminServer) registerRoutes() {
	server := s.newAdminAPIServer()
	httpx.MustGet(server, "/healthz", s.apiHealth)
	httpx.MustGet(server, "/topics", s.apiTopics)
	httpx.MustGet(server, "/metrics", s.apiMetrics)
	httpx.MustGet(server, "/quota", s.apiQuota)
	httpx.MustGet(server, "/cluster", s.apiCluster)
	httpx.MustPost(server, "/gateway/topics/{topic}/records", s.apiGatewayProduce)
	httpx.MustGet(server, "/gateway/topics/{topic}/partitions/{partition}/records", s.apiGatewayFetch)
	httpx.MustPost(server, "/gateway/topics/{topic}/partitions/{partition}/commit", s.apiGatewayCommit)
	s.registerDebugAPIRoutes(server)
	s.registerUIRoutes()
	s.registerLegacyRoutes()
	s.registerConsumerGroupRoutes()
	s.registerACLRoutes()
	s.registerOperationRoutes()
	s.registerClusterUIRoutes()
	s.registerClusterControlRoutes()
}

func (s *AdminServer) newAdminAPIServer() httpx.ServerRuntime {
	adminAdapter := httpxfiber.New(s.app, adapter.HumaOptions{
		Title:       "ech0 Admin API",
		Version:     "0.1.0",
		Description: "Operational API for ech0 broker nodes.",
		DocsPath:    "/docs",
		OpenAPIPath: "/openapi.json",
	})
	return httpx.New(
		httpx.WithAdapter(adminAdapter),
		httpx.WithBasePath("/api"),
		httpx.WithValidation(),
	)
}

func (s *AdminServer) registerDebugAPIRoutes(server httpx.ServerRuntime) {
	if !s.cfg.Admin.DebugEnabled {
		return
	}
	httpx.MustGet(server, "/runtime/events", s.apiRuntimeEvents)
	httpx.MustGetSSE(server, "/runtime/events/stream", map[string]any{
		"runtime_events": runtimeEventsStreamEvent{},
	}, s.apiRuntimeEventsStream)
}

func (s *AdminServer) registerUIRoutes() {
	s.app.Get("/", s.redirectRoot)
	s.app.Get("/ui", s.uiDashboard)
	s.app.Get("/ui/topics", s.uiTopics)
	s.app.Get("/ui/governance", s.uiGovernance)
	s.app.Get("/ui/acls", s.uiACLPolicies)
	s.app.Get("/ui/ops", s.uiOperations)
	s.app.Get("/ui/cluster", s.uiCluster)
	s.app.Get("/ui/topics/:topic/messages", s.uiTopicMessages)
	s.app.Get("/ui/groups/:group", s.uiGroup)
}

func (s *AdminServer) registerLegacyRoutes() {
	s.app.Get("/healthz", s.legacyHealth)
	s.app.Get("/metrics", s.prometheusMetrics)
	if s.cfg.Admin.DebugEnabled {
		s.app.Get("/debug/fgprof", fiberadaptor.HTTPHandler(fgprof.Handler()))
	}
}

func (s *AdminServer) registerConsumerGroupRoutes() {
	s.app.Get("/api/groups/:group/members", s.apiGroupMembers)
	s.app.Get("/api/groups/:group/health", s.apiGroupHealth)
	s.app.Get("/api/groups/:group/assignment", s.apiGroupAssignment)
	s.app.Get("/api/groups/:group/lag", s.apiGroupLag)
	s.app.Post("/api/groups/:group/rebalance", s.apiGroupRebalance)
	s.app.Get("/api/groups/:group/rebalance-explain", s.apiGroupRebalanceExplain)
}

func (s *AdminServer) registerACLRoutes() {
	s.app.Get("/api/acl/policies", s.apiACLPolicies)
	s.app.Post("/api/acl/policies", s.apiACLPolicyUpsert)
	s.app.Post("/api/acl/policies/delete", s.apiACLPolicyDelete)
}

func (s *AdminServer) registerOperationRoutes() {
	s.app.Post("/api/ops/webhook-sinks/run", s.apiRunWebhookSink)
	s.app.Post("/api/ops/file-sinks/run", s.apiRunFileSink)
	s.app.Post("/api/ops/mirror-sinks/run", s.apiRunMirrorSink)
	s.app.Post("/api/ops/s3-sinks/run", s.apiRunS3Sink)
	s.app.Post("/api/ops/database-outboxes/run", s.apiRunDatabaseOutbox)
}

func (s *AdminServer) registerClusterUIRoutes() {
	s.app.Post("/ui/cluster/nodes/join", s.uiClusterNodeJoin)
	s.app.Post("/ui/cluster/nodes/leave", s.uiClusterNodeLeave)
	s.app.Post("/ui/cluster/leadership/transfer", s.uiClusterLeadershipTransfer)
	s.app.Post("/ui/cluster/leadership/balance", s.uiClusterLeadershipBalance)
	s.app.Post("/ui/cluster/partitions/reassign", s.uiClusterPartitionReassign)
}
