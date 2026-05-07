package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/DaiYuANg/ech0/protocol"
	"github.com/DaiYuANg/ech0/transport"
)

type TCPServer struct {
	addr     string
	limits   BrokerConfig
	broker   *Broker
	logger   *slog.Logger
	metrics  *MetricsRuntime
	listener net.Listener
	wg       sync.WaitGroup
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
	s.wg.Add(1)
	go s.acceptLoop(ctx)
	return nil
}

func (s *TCPServer) Stop(ctx context.Context) error {
	var closeErr error
	if s.listener != nil {
		closeErr = s.listener.Close()
	}
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return errors.Join(
			wrapBroker("tcp_stop_context_done", ctx.Err(), "stop tcp server"),
			wrapBroker("tcp_listener_close_failed", closeErr, "close tcp listener"),
		)
	case <-done:
		return wrapBroker("tcp_listener_close_failed", closeErr, "close tcp listener")
	}
}

func (s *TCPServer) acceptLoop(ctx context.Context) {
	defer s.wg.Done()
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return
			}
			if s.logger != nil {
				s.logger.Warn("accept failed", "error", err)
			}
			continue
		}
		s.metrics.RecordTCPConnection(ctx)
		s.wg.Add(1)
		go s.handleConn(ctx, conn)
	}
}

func (s *TCPServer) handleConn(ctx context.Context, conn net.Conn) {
	defer s.wg.Done()
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
	frame, err := transport.ReadFrameWithLimit(conn, s.limits.MaxFrameBodyBytes)
	if err != nil {
		s.logReadFrameError(err)
		return false
	}
	response, err := s.HandleFrame(ctx, frame)
	if err != nil {
		response = errorFrame("internal_error", err.Error())
	}
	s.recordCommandError(ctx, response)
	return s.writeResponseFrame(conn, response)
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
	if frame.Header.Version != protocol.Version1 {
		return errorFrame("unsupported_version", fmt.Sprintf("unsupported protocol version %d", frame.Header.Version)), nil
	}
	s.metrics.RecordCommand(ctx, frame.Header.Command)
	handler, ok := tcpFrameHandlers[frame.Header.Command]
	if !ok {
		return errorFrame("unsupported_command", fmt.Sprintf("unsupported command %d", frame.Header.Command)), nil
	}
	return handler(s, ctx, frame)
}
