// Package broker contains the embeddable broker runtime and operator surfaces.
package broker

import (
	"context"
	"embed"
	"log/slog"
	"strings"
	"sync"
	"time"

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
	cfg     Config
	broker  *Broker
	logger  *slog.Logger
	metrics *MetricsRuntime
	app     *fiber.App
	once    sync.Once
}

func NewAdminServer(cfg Config, broker *Broker, logger *slog.Logger, metrics *MetricsRuntime) *AdminServer {
	if metrics == nil {
		metrics = NewNoopMetricsRuntime(logger)
	}
	return &AdminServer{cfg: cfg, broker: broker, logger: logger, metrics: metrics}
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

	s.app.Get("/", s.redirectRoot)
	s.app.Get("/ui", s.uiDashboard)
	s.app.Get("/ui/topics", s.uiTopics)
	s.app.Get("/ui/topics/:topic/messages", s.uiTopicMessages)
	s.app.Get("/ui/groups/:group", s.uiGroup)

	s.app.Get("/healthz", s.legacyHealth)
	s.app.Get("/metrics", s.prometheusMetrics)
	if s.cfg.Admin.DebugEnabled {
		s.app.Get("/debug/fgprof", fiberadaptor.HTTPHandler(fgprof.Handler()))
	}
	s.app.Get("/api/groups/:group/members", s.apiGroupMembers)
	s.app.Get("/api/groups/:group/assignment", s.apiGroupAssignment)
	s.app.Get("/api/groups/:group/lag", s.apiGroupLag)
	s.app.Post("/api/groups/:group/rebalance", s.apiGroupRebalance)
	s.app.Get("/api/groups/:group/rebalance-explain", s.apiGroupRebalanceExplain)
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

type topicMessagesView struct {
	Page       TopicMessagesPageSummary
	PrevOffset uint64
	Error      string
}

type groupView struct {
	Group      string
	Explain    *GroupRebalanceExplainSummary
	Members    []GroupMemberSummary
	Assignment *GroupAssignmentSummary
	Lag        *GroupLagSummary
	Error      string
}
