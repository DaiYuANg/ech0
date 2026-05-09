package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"

	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/transport"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

var (
	errTCPAcceptRetry   = errors.New("retry tcp accept")
	errTCPServerStopped = errors.New("tcp server stopped")
)

type TCPServer struct {
	addr     string
	limits   BrokerConfig
	broker   *Broker
	logger   *slog.Logger
	metrics  *MetricsRuntime
	listener net.Listener
	group    *errgroup.Group
	connSem  *semaphore.Weighted
	limiter  *rate.Limiter
	clients  *tcpConnectionIndex
}

func NewTCPServer(cfg Config, broker *Broker, logger *slog.Logger, metrics *MetricsRuntime) *TCPServer {
	if metrics == nil {
		metrics = NewNoopMetricsRuntime(logger)
	}
	return &TCPServer{
		addr:    cfg.Broker.BindAddr,
		limits:  cfg.Broker,
		broker:  broker,
		logger:  logger,
		metrics: metrics,
		connSem: newConnectionSemaphore(cfg.Broker.MaxConcurrentConnections),
		limiter: newCommandRateLimiter(cfg.Broker.CommandRateLimitPerSecond, cfg.Broker.CommandRateLimitBurst),
		clients: newTCPConnectionIndex(),
	}
}

func (s *TCPServer) Start(ctx context.Context) error {
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return wrapBroker("tcp_listen_failed", err, "listen tcp broker")
	}
	s.listener = listener
	if s.logger != nil {
		s.logger.Info("tcp broker listening", "addr", s.addr)
	}
	group, groupCtx := errgroup.WithContext(ctx)
	s.group = group
	group.Go(func() error {
		return s.acceptLoop(groupCtx)
	})
	return nil
}

func (s *TCPServer) Stop(ctx context.Context) error {
	var closeErr error
	if s.listener != nil {
		closeErr = s.listener.Close()
	}
	if s.group == nil {
		return wrapBroker("tcp_listener_close_failed", closeErr, "close tcp listener")
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = normalizeTCPServerWaitError(s.group.Wait())
		close(done)
	}()
	select {
	case <-ctx.Done():
		return errors.Join(
			wrapBroker("tcp_stop_context_done", ctx.Err(), "stop tcp server"),
			wrapBroker("tcp_listener_close_failed", closeErr, "close tcp listener"),
		)
	case <-done:
		return errors.Join(
			wrapBroker("tcp_listener_close_failed", closeErr, "close tcp listener"),
			waitErr,
		)
	}
}

func (s *TCPServer) acceptLoop(ctx context.Context) error {
	for {
		err := s.acceptConnection(ctx)
		if err == nil || errors.Is(err, errTCPAcceptRetry) {
			continue
		}
		return err
	}
}

func (s *TCPServer) acceptConnection(ctx context.Context) error {
	conn, err := s.listener.Accept()
	if err != nil {
		return s.acceptError(ctx, err)
	}
	if err := s.acquireConnection(ctx); err != nil {
		return s.connectionAcquireError(ctx, conn, err)
	}
	s.startConnectionHandler(ctx, conn)
	return nil
}

func (s *TCPServer) acceptError(ctx context.Context, err error) error {
	if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
		return errTCPServerStopped
	}
	if s.logger != nil {
		s.logger.Warn("accept failed", "error", err)
	}
	return errTCPAcceptRetry
}

func (s *TCPServer) connectionAcquireError(ctx context.Context, conn net.Conn, err error) error {
	s.closeRejectedConnection(conn, err)
	if ctx.Err() != nil {
		return errTCPServerStopped
	}
	return errTCPAcceptRetry
}

func (s *TCPServer) startConnectionHandler(ctx context.Context, conn net.Conn) {
	s.metrics.RecordTCPConnection(ctx)
	s.group.Go(func() error {
		s.handleConn(ctx, conn)
		return nil
	})
}

func (s *TCPServer) handleConn(ctx context.Context, conn net.Conn) {
	defer s.releaseConnection()
	defer s.unregisterConnection(conn)
	defer func() {
		if err := conn.Close(); err != nil && s.logger != nil {
			s.logger.Debug("close connection failed", "error", err)
		}
	}()
	for {
		if !s.handleConnFrame(ctx, conn) {
			return
		}
	}
}

func (s *TCPServer) handleConnFrame(ctx context.Context, conn net.Conn) bool {
	if err := s.waitCommandSlot(ctx); err != nil {
		if s.logger != nil {
			s.logger.Debug("command rate limiter stopped", "error", err)
		}
		return false
	}
	frame, err := transport.ReadFrameWithLimit(conn, s.limits.MaxFrameBodyBytes)
	if err != nil {
		s.logReadFrameError(err)
		return false
	}
	s.registerConnectionHandshake(conn, frame)
	handleStart := time.Now()
	response, err := s.HandleFrame(ctx, frame)
	if err != nil {
		response = errorFrame("internal_error", err.Error())
	}
	s.metrics.RecordCommandDuration(ctx, frame.Header.Command, time.Since(handleStart), commandStatus(response))
	s.recordCommandError(ctx, response)
	return s.writeResponseFrame(conn, response)
}

func commandStatus(response transport.Frame) string {
	if response.Header.Status == transport.StatusError || response.Header.Command == protocol.CmdErrorResponse {
		return "error"
	}
	return "ok"
}

func (s *TCPServer) logReadFrameError(err error) {
	if !errors.Is(err, io.EOF) && s.logger != nil {
		s.logger.Debug("read frame failed", "error", err)
	}
}

func (s *TCPServer) writeResponseFrame(conn net.Conn, response transport.Frame) bool {
	if err := transport.WriteFrame(conn, response); err != nil {
		if s.logger != nil {
			s.logger.Debug("write frame failed", "error", err)
		}
		return false
	}
	return true
}

func (s *TCPServer) HandleFrame(ctx context.Context, frame transport.Frame) (transport.Frame, error) {
	if frame.Header.Version != protocol.Version {
		return errorFrame("unsupported_version", fmt.Sprintf("unsupported protocol version %d", frame.Header.Version)), nil
	}
	s.metrics.RecordCommand(ctx, frame.Header.Command)
	handler, ok := tcpFrameHandlers[frame.Header.Command]
	if !ok {
		return errorFrame("unsupported_command", fmt.Sprintf("unsupported command %d", frame.Header.Command)), nil
	}
	return handler(s, ctx, frame)
}

func newConnectionSemaphore(limit int64) *semaphore.Weighted {
	if limit <= 0 {
		return nil
	}
	return semaphore.NewWeighted(limit)
}

func newCommandRateLimiter(limitPerSecond float64, burst int) *rate.Limiter {
	if limitPerSecond <= 0 {
		return nil
	}
	if burst <= 0 {
		burst = int(limitPerSecond)
	}
	if burst <= 0 {
		burst = 1
	}
	return rate.NewLimiter(rate.Limit(limitPerSecond), burst)
}

func (s *TCPServer) acquireConnection(ctx context.Context) error {
	if s.connSem == nil {
		return nil
	}
	return wrapBroker("tcp_connection_limit_acquire_failed", s.connSem.Acquire(ctx, 1), "acquire tcp connection slot")
}

func (s *TCPServer) releaseConnection() {
	if s.connSem != nil {
		s.connSem.Release(1)
	}
}

func (s *TCPServer) waitCommandSlot(ctx context.Context) error {
	if s.limiter == nil {
		return nil
	}
	return wrapBroker("tcp_command_rate_wait_failed", s.limiter.Wait(ctx), "wait tcp command rate limiter")
}

func (s *TCPServer) closeRejectedConnection(conn net.Conn, cause error) {
	if s.logger != nil {
		s.logger.Warn("reject tcp connection", "error", cause)
	}
	if err := conn.Close(); err != nil && s.logger != nil {
		s.logger.Debug("close rejected connection failed", "error", err)
	}
}

func normalizeTCPServerWaitError(err error) error {
	if errors.Is(err, errTCPServerStopped) {
		return nil
	}
	return err
}
