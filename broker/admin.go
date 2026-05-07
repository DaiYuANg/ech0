// Package broker contains the ech0 broker runtime.
package broker

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/arcgolabs/httpx"
	"github.com/arcgolabs/httpx/adapter"
	httpxfiber "github.com/arcgolabs/httpx/adapter/fiber"
	"github.com/gofiber/fiber/v2"
	"github.com/samber/oops"
)

type AdminServer struct {
	cfg    Config
	broker *Broker
	logger *slog.Logger
	app    *fiber.App
	once   sync.Once
}

func NewAdminServer(cfg Config, broker *Broker, logger *slog.Logger) *AdminServer {
	return &AdminServer{cfg: cfg, broker: broker, logger: logger}
}

func (s *AdminServer) Start(ctx context.Context) error {
	if !s.cfg.Admin.Enabled {
		return nil
	}
	s.once.Do(func() {
		s.app = fiber.New(fiber.Config{
			AppName: "ech0-admin",
		})
		s.registerRoutes()
	})
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
	httpx.MustGet(server, "/healthz", func(ctx context.Context, _ *struct{}) (*healthOutput, error) {
		_ = ctx
		out := &healthOutput{}
		out.Body = s.broker.RuntimeHealth()
		return out, nil
	})
	httpx.MustGet(server, "/topics", func(ctx context.Context, _ *struct{}) (*topicsOutput, error) {
		_ = ctx
		topics, err := s.broker.ListTopics()
		if err != nil {
			return nil, wrapBroker("list_topics_failed", err, "list topics")
		}
		out := &topicsOutput{}
		out.Body.Topics = topics
		return out, nil
	})
	httpx.MustGet(server, "/metrics", func(ctx context.Context, _ *struct{}) (*metricsOutput, error) {
		_ = ctx
		topics, err := s.broker.ListTopics()
		if err != nil {
			return nil, oops.In("admin").Code("list_topics_failed").Wrapf(err, "build metrics")
		}
		out := &metricsOutput{}
		out.Body.TopicCount = len(topics)
		out.Body.Runtime = s.broker.RuntimeHealth().RuntimeMode
		return out, nil
	})

	s.app.Get("/healthz", func(c *fiber.Ctx) error {
		return c.JSON(s.broker.RuntimeHealth())
	})
	s.app.Get("/metrics", func(c *fiber.Ctx) error {
		topics, err := s.broker.ListTopics()
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).SendString(err.Error())
		}
		c.Set("Content-Type", "text/plain; version=0.0.4")
		return c.SendString("ech0_topics_total " + itoa(len(topics)) + "\n")
	})
}

type healthOutput struct {
	Body RuntimeHealth `json:"body"`
}

type topicsOutput struct {
	Body struct {
		Topics any `json:"topics"`
	} `json:"body"`
}

type metricsOutput struct {
	Body struct {
		TopicCount int    `json:"topic_count"`
		Runtime    string `json:"runtime"`
	} `json:"body"`
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	n := v
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
