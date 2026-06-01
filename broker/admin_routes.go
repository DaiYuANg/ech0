package broker

import (
	"github.com/felixge/fgprof"
	fiberadaptor "github.com/gofiber/fiber/v3/middleware/adaptor"
)

func (s *AdminServer) registerRoutes() {
	s.registerAPIRoutes()
	s.registerDebugAPIRoutes()
	s.registerUIRoutes()
	s.registerLegacyRoutes()
	s.registerConsumerGroupRoutes()
	s.registerACLRoutes()
	s.registerOperationRoutes()
	s.registerClusterUIRoutes()
	s.registerClusterControlRoutes()
}

func (s *AdminServer) registerAPIRoutes() {
	s.app.Get("/docs", s.apiDocs)
	s.app.Get("/openapi.json", s.apiOpenAPI)
	s.app.Get("/api/healthz", s.apiHealth)
	s.app.Get("/api/topics", s.apiTopics)
	s.app.Get("/api/metrics", s.apiMetrics)
	s.app.Get("/api/quota", s.apiQuota)
	s.app.Get("/api/cluster", s.apiCluster)
	s.app.Post("/api/gateway/topics/:topic/records", s.apiGatewayProduce)
	s.app.Get("/api/gateway/topics/:topic/partitions/:partition/records", s.apiGatewayFetch)
	s.app.Post("/api/gateway/topics/:topic/partitions/:partition/commit", s.apiGatewayCommit)
}

func (s *AdminServer) registerDebugAPIRoutes() {
	if !s.cfg.Admin.DebugEnabled {
		return
	}
	s.app.Get("/api/runtime/events", s.apiRuntimeEvents)
	s.app.Get("/api/runtime/events/stream", s.apiRuntimeEventsStream)
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
