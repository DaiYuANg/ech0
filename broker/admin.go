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
	"github.com/gofiber/fiber/v3"
	fiberadaptor "github.com/gofiber/fiber/v3/middleware/adaptor"
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

func (s *AdminServer) redirectRoot(c fiber.Ctx) error {
	return wrapBroker("admin_redirect_failed", c.Redirect().Status(fiber.StatusTemporaryRedirect).To("/ui"), "redirect admin root")
}

func (s *AdminServer) legacyHealth(c fiber.Ctx) error {
	return adminJSON(c, s.broker.RuntimeHealth())
}

func (s *AdminServer) prometheusMetrics(c fiber.Ctx) error {
	if err := s.metrics.RefreshStream(c.Context(), s.broker); err != nil {
		return wrapBroker("admin_metrics_refresh_failed", c.Status(fiber.StatusInternalServerError).SendString(err.Error()), "write metrics refresh error")
	}
	return wrapBroker("admin_metrics_write_failed", fiberadaptor.HTTPHandler(s.metrics.Handler())(c), "write prometheus metrics")
}

func (s *AdminServer) adminIdentityMiddleware(c fiber.Ctx) error {
	identity := identityFromContext(c.Context())
	identity.Tenant = adminIdentityValue(c, "X-Ech0-Tenant", "tenant", identity.Tenant)
	identity.Namespace = adminIdentityValue(c, "X-Ech0-Namespace", "namespace", identity.Namespace)
	identity.Principal = adminIdentityValue(c, "X-Ech0-Principal", "principal", identity.Principal)
	identity.ClientID = adminIdentityValue(c, "X-Ech0-Client-Id", "client_id", identity.ClientID)
	if s.broker != nil && s.broker.cfg.Governance.Auth.Enabled {
		authenticated, err := s.broker.authenticate(c.Context(), AuthRequest{
			ClientID:  identity.ClientID,
			Principal: identity.Principal,
			Tenant:    identity.Tenant,
			Namespace: identity.Namespace,
			Token:     adminAuthToken(c),
		})
		if err != nil {
			return adminAuthError(c, err)
		}
		identity = authenticated
	}
	c.SetContext(WithIdentity(c.Context(), identity))
	return wrapBroker("admin_identity_middleware_failed", c.Next(), "run admin identity middleware")
}

func adminAuthToken(c fiber.Ctx) string {
	token := strings.TrimSpace(c.Get("X-Ech0-Auth-Token"))
	if token != "" {
		return token
	}
	header := strings.TrimSpace(c.Get("Authorization"))
	if before, after, ok := strings.Cut(header, " "); ok && strings.EqualFold(before, "Bearer") {
		token = strings.TrimSpace(after)
		if token != "" {
			return token
		}
	}
	return strings.TrimSpace(c.Query("auth_token"))
}

func adminAuthError(c fiber.Ctx, err error) error {
	response := adminErrorResponse{Error: "unauthorized"}
	return wrapBroker("admin_auth_failed", c.Status(fiber.StatusUnauthorized).JSON(response), "authenticate admin request: %v", err)
}

func adminIdentityValue(c fiber.Ctx, header, query, fallback string) string {
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
	Quota            QuotaSummary
	Topics           []TopicSummary
	TopicsError      string
	QuotaError       string
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
