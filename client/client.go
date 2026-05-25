package client

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clienttcp "github.com/arcgolabs/clientx/tcp"
	"github.com/lyonbrown4d/ech0/protocol"
	protocolbinary "github.com/lyonbrown4d/ech0/protocol/binary"
	"github.com/lyonbrown4d/ech0/transport"
	"github.com/samber/oops"
)

type Client struct {
	addr      string
	opts      Options
	pool      chan net.Conn
	requestID atomic.Uint64
	closed    atomic.Bool
	serverMu  sync.RWMutex
	server    protocol.HandshakeResponse
	tcpClient clienttcp.Client
}

func Dial(ctx context.Context, addr string, opts ...Option) (*Client, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return nil, oops.In("client").Code("addr_required").New("broker address is required")
	}
	normalized := normalizeOptions(opts)
	tcpClient, err := newTCPClient(addr, normalized)
	if err != nil {
		return nil, err
	}
	client := &Client{
		addr:      addr,
		opts:      normalized,
		tcpClient: tcpClient,
	}
	client.pool = make(chan net.Conn, client.opts.PoolSize)
	conn, err := client.dial(ctx)
	if err != nil {
		return nil, errors.Join(err, wrap("tcp_client_close_failed", tcpClient.Close(), "close tcp client after dial failure"))
	}
	client.release(conn)
	return client, nil
}

func (c *Client) Close(context.Context) error {
	if c == nil || c.closed.Swap(true) {
		return nil
	}
	var result error
	for {
		select {
		case conn := <-c.pool:
			result = errors.Join(result, conn.Close())
		default:
			if c.tcpClient != nil {
				result = errors.Join(result, c.tcpClient.Close())
			}
			return wrap("client_close_failed", result, "close client")
		}
	}
}

func (c *Client) Server() protocol.HandshakeResponse {
	if c == nil {
		return protocol.HandshakeResponse{}
	}
	c.serverMu.RLock()
	defer c.serverMu.RUnlock()
	return c.server
}

func (c *Client) RoundTrip(ctx context.Context, requestCommand, responseCommand uint16, request, response any) error {
	if err := c.validate(); err != nil {
		return err
	}
	frame, err := c.newRequestFrame(requestCommand, request)
	if err != nil {
		return err
	}
	conn, err := c.borrow(ctx)
	if err != nil {
		return err
	}
	release := false
	defer c.finishConn(conn, &release)
	release, err = c.roundTripFrame(ctx, conn, frame, responseCommand, response)
	return err
}

func (c *Client) Ping(ctx context.Context, nonce uint64) (protocol.PingResponse, error) {
	var out protocol.PingResponse
	err := c.RoundTrip(ctx, protocol.CmdPingRequest, protocol.CmdPingResponse, protocol.PingRequest{Nonce: nonce}, &out)
	return out, err
}

func (c *Client) validate() error {
	if c == nil {
		return oops.In("client").Code("client_nil").New("client is nil")
	}
	if c.closed.Load() {
		return oops.In("client").Code("client_closed").New("client is closed")
	}
	return nil
}

func (c *Client) newRequestFrame(command uint16, request any) (transport.Frame, error) {
	body, err := protocolbinary.EncodeBody(command, request)
	if err != nil {
		return transport.Frame{}, wrap("request_encode_failed", err, "encode request body")
	}
	frame, err := transport.NewFrame(protocol.Version, command, body)
	if err != nil {
		return transport.Frame{}, wrap("request_frame_failed", err, "create request frame")
	}
	frame.Header.RequestID = c.requestID.Add(1)
	return frame, nil
}

func (c *Client) roundTripFrame(
	ctx context.Context,
	conn net.Conn,
	frame transport.Frame,
	responseCommand uint16,
	response any,
) (bool, error) {
	if err := c.setDeadline(ctx, conn); err != nil {
		return false, err
	}
	if err := transport.WriteFrame(conn, frame); err != nil {
		return false, wrap("frame_write_failed", err, "write request frame")
	}
	out, err := transport.ReadFrameWithLimitPooled(conn, c.opts.MaxFrameBodyBytes)
	if err != nil {
		return false, wrap("frame_read_failed", err, "read response frame")
	}
	defer out.Release()
	keep, decodeErr := decodeResponse(out.Frame, responseCommand, response)
	return keep, decodeErr
}

func decodeResponse(frame transport.Frame, expectedCommand uint16, target any) (bool, error) {
	if frame.Header.Status == transport.StatusError || frame.Header.Command == protocol.CmdErrorResponse {
		return decodeBrokerError(frame)
	}
	if frame.Header.Command != expectedCommand {
		err := oops.In("client").Code("unexpected_response").With("command", frame.Header.Command, "expected", expectedCommand).New("unexpected response command")
		return false, err
	}
	if target == nil {
		return true, nil
	}
	if err := protocolbinary.DecodeBody(frame.Header.Command, frame.Body, target); err != nil {
		return false, wrap("response_decode_failed", err, "decode response body")
	}
	return true, nil
}

func decodeBrokerError(frame transport.Frame) (bool, error) {
	var out protocol.ErrorResponse
	if err := protocolbinary.DecodeBody(protocol.CmdErrorResponse, frame.Body, &out); err != nil {
		return false, wrap("broker_error_decode_failed", err, "decode broker error")
	}
	return true, brokerErrorFromProtocol(out)
}

func (c *Client) finishConn(conn net.Conn, release *bool) {
	if release != nil && *release {
		c.release(conn)
		return
	}
	closeConn(conn)
}

func (c *Client) setDeadline(ctx context.Context, conn net.Conn) error {
	deadline, ok := ctx.Deadline()
	if !ok && c.opts.OperationTimeout > 0 {
		deadline = time.Now().Add(c.opts.OperationTimeout)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		return wrap("deadline_failed", err, "set connection deadline")
	}
	return nil
}
