// Package broker contains the embeddable broker runtime and operator surfaces.
package broker

import (
	"context"
	"embed"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/arcgolabs/dix"
	"github.com/arcgolabs/httpx"
	"github.com/arcgolabs/httpx/adapter"
	httpxfiber "github.com/arcgolabs/httpx/adapter/fiber"
	"github.com/felixge/fgprof"
	"github.com/gofiber/fiber/v2"
	fiberadaptor "github.com/gofiber/fiber/v2/middleware/adaptor"
)

//go:embed admin_templates/*.html
var adminTemplates embed.FS

type AdminServer struct {
	cfg          Config
	broker       *Broker
	logger       *slog.Logger
	metrics      *MetricsRuntime
	events       *dix.EventRecorder
	brokerEvents *BrokerEventRecorder
	app          *fiber.App
	once         sync.Once
}

func NewAdminServer(cfg Config, broker *Broker, logger *slog.Logger, metrics *MetricsRuntime, events ...*dix.EventRecorder) *AdminServer {
	if metrics == nil {
		metrics = NewNoopMetricsRuntime(logger)
	}
	var recorder *dix.EventRecorder
	if len(events) > 0 {
		recorder = events[0]
	}
	return &AdminServer{cfg: cfg, broker: broker, logger: logger, metrics: metrics, events: recorder}
}

func (s *AdminServer) Start(ctx context.Context) error {
	if !s.cfg.Admin.Enabled {
		return nil
	}
	s.ensureApp()
	if s.logger != nil {
		s.logger.Info("admin http listening", "addr", s.cfg.Admin.BindAddr, "runtime", "fiber")
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.app.Listen(s.cfg.Admin.BindAddr)
	}()
	select {
	case <-ctx.Done():
		return wrapBroker("admin_start_canceled", ctx.Err(), "start admin server")
	case err := <-errCh:
		if err != nil && !strings.Contains(err.Error(), "server is not running") {
			return wrapBroker("admin_listen_failed", err, "listen admin server")
		}
		return nil
	case <-time.After(50 * time.Millisecond):
		return nil
	}
}

func (s *AdminServer) ensureApp() {
	s.once.Do(s.initApp)
}

func (s *AdminServer) initApp() {
	s.app = fiber.New(fiber.Config{
		AppName: "ech0-admin",
		Views:   adminTemplateEngine(),
	})
	s.app.Use(s.adminIdentityMiddleware)
	s.registerRoutes()
}

func (s *AdminServer) Stop(ctx context.Context) error {
	if s.app == nil {
		return nil
	}
	return wrapBroker("admin_shutdown_failed", s.app.ShutdownWithContext(ctx), "shutdown admin server")
}

func (s *AdminServer) registerRoutes() {
	adminAdapter := httpxfiber.New(s.app, adapter.HumaOptions{
		Title:       "ech0 Admin API",
		Version:     "0.1.0",
		Description: "Operational API for ech0 broker nodes.",
		DocsPath:    "/docs",
		OpenAPIPath: "/openapi.json",
	})
	server := httpx.New(
		httpx.WithAdapter(adminAdapter),
		httpx.WithBasePath("/api"),
		httpx.WithValidation(),
	)
	httpx.MustGet(server, "/healthz", s.apiHealth)
	httpx.MustGet(server, "/topics", s.apiTopics)
	httpx.MustGet(server, "/metrics", s.apiMetrics)
	if s.cfg.Admin.DebugEnabled {
		httpx.MustGet(server, "/runtime/events", s.apiRuntimeEvents)
		httpx.MustGetSSE(server, "/runtime/events/stream", map[string]any{
			"runtime_events": runtimeEventsStreamEvent{},
		}, s.apiRuntimeEventsStream)
	}

	s.app.Get("/", s.redirectRoot)
	s.app.Get("/ui", s.uiDashboard)
	s.app.Get("/ui/topics", s.uiTopics)
	s.app.Get("/ui/acls", s.uiACLPolicies)
	s.app.Get("/ui/topics/:topic/messages", s.uiTopicMessages)
	s.app.Get("/ui/groups/:group", s.uiGroup)

	s.app.Get("/healthz", s.legacyHealth)
	s.app.Get("/metrics", s.prometheusMetrics)
	if s.cfg.Admin.DebugEnabled {
		s.app.Get("/debug/fgprof", fiberadaptor.HTTPHandler(fgprof.Handler()))
	}
	s.app.Get("/api/groups/:group/members", s.apiGroupMembers)
	s.app.Get("/api/groups/:group/health", s.apiGroupHealth)
	s.app.Get("/api/groups/:group/assignment", s.apiGroupAssignment)
	s.app.Get("/api/groups/:group/lag", s.apiGroupLag)
	s.app.Post("/api/groups/:group/rebalance", s.apiGroupRebalance)
	s.app.Get("/api/groups/:group/rebalance-explain", s.apiGroupRebalanceExplain)
	s.app.Get("/api/acl/policies", s.apiACLPolicies)
	s.app.Post("/api/acl/policies", s.apiACLPolicyUpsert)
	s.app.Post("/api/acl/policies/delete", s.apiACLPolicyDelete)
}

func (s *AdminServer) apiRuntimeEventsStream(ctx context.Context, _ *struct{}, send httpx.SSESender) {
	if !s.sendRuntimeEventsSnapshot(send) {
		return
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !s.sendRuntimeEventsSnapshot(send) {
				return
			}
		}
	}
}

func (s *AdminServer) sendRuntimeEventsSnapshot(send httpx.SSESender) bool {
	if err := send.Data(s.runtimeEventStreamSnapshot()); err != nil {
		if s.logger != nil {
			s.logger.Debug("send runtime event stream snapshot failed", "error", err)
		}
		return false
	}
	return true
}

func (s *AdminServer) redirectRoot(c *fiber.Ctx) error {
	return wrapBroker("admin_redirect_failed", c.Redirect("/ui", fiber.StatusTemporaryRedirect), "redirect admin root")
}

func (s *AdminServer) legacyHealth(c *fiber.Ctx) error {
	return adminJSON(c, s.broker.RuntimeHealth())
}

func (s *AdminServer) prometheusMetrics(c *fiber.Ctx) error {
	if err := s.metrics.RefreshStream(c.UserContext(), s.broker); err != nil {
		return wrapBroker("admin_metrics_refresh_failed", c.Status(fiber.StatusInternalServerError).SendString(err.Error()), "write metrics refresh error")
	}
	return wrapBroker("admin_metrics_write_failed", fiberadaptor.HTTPHandler(s.metrics.Handler())(c), "write prometheus metrics")
}

func (s *AdminServer) adminIdentityMiddleware(c *fiber.Ctx) error {
	identity := identityFromContext(c.UserContext())
	identity.Tenant = adminIdentityValue(c, "X-Ech0-Tenant", "tenant", identity.Tenant)
	identity.Namespace = adminIdentityValue(c, "X-Ech0-Namespace", "namespace", identity.Namespace)
	identity.Principal = adminIdentityValue(c, "X-Ech0-Principal", "principal", identity.Principal)
	identity.ClientID = adminIdentityValue(c, "X-Ech0-Client-Id", "client_id", identity.ClientID)
	c.SetUserContext(WithIdentity(c.UserContext(), identity))
	return wrapBroker("admin_identity_middleware_failed", c.Next(), "run admin identity middleware")
}

func adminIdentityValue(c *fiber.Ctx, header, query, fallback string) string {
	value := strings.TrimSpace(c.Get(header))
	if value != "" {
		return value
	}
	value = strings.TrimSpace(c.Query(query))
	if value != "" {
		return value
	}
	return fallback
}

type dashboardView struct {
	Health           RuntimeHealth
	Metrics          MetricsSnapshot
	Topics           []TopicSummary
	TopicsError      string
	CommandErrorRate string
}

type topicsView struct {
	Topics []TopicSummary
	Error  string
}

type aclPoliciesView struct {
	Policies  []ACLPolicy
	Error     string
	Tenant    string
	Namespace string
	Principal string
}

type topicMessagesView struct {
	Page       TopicMessagesPageSummary
	PrevOffset uint64
	Error      string
}

type groupView struct {
	Group      string
	Health     *GroupHealthSummary
	Explain    *GroupRebalanceExplainSummary
	Members    []GroupMemberSummary
	Assignment *GroupAssignmentSummary
	Lag        *GroupLagSummary
	Error      string
}
