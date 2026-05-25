package client

import (
	"context"
	"net"

	clienttcp "github.com/arcgolabs/clientx/tcp"
	"github.com/lyonbrown4d/ech0/protocol"
)

func (c *Client) borrow(ctx context.Context) (net.Conn, error) {
	select {
	case conn := <-c.pool:
		return conn, nil
	default:
		return c.dial(ctx)
	}
}

func (c *Client) release(conn net.Conn) {
	if conn == nil || c.closed.Load() {
		closeConn(conn)
		return
	}
	select {
	case c.pool <- conn:
	default:
		closeConn(conn)
	}
}

func (c *Client) dial(ctx context.Context) (net.Conn, error) {
	conn, err := c.tcpClient.Dial(ctx)
	if err != nil {
		return nil, wrap("dial_failed", err, "dial broker")
	}
	if err := configureConn(conn); err != nil {
		closeConn(conn)
		return nil, err
	}
	if err := c.handshake(ctx, conn); err != nil {
		closeConn(conn)
		return nil, err
	}
	return conn, nil
}

func newTCPClient(addr string, opts Options) (clienttcp.Client, error) {
	tcpClient, err := clienttcp.New(clienttcp.Config{
		Network:     "tcp",
		Address:     addr,
		DialTimeout: opts.DialTimeout,
	})
	if err != nil {
		return nil, wrap("tcp_client_create_failed", err, "create tcp client")
	}
	return tcpClient, nil
}

func configureConn(conn net.Conn) error {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	if err := tcpConn.SetNoDelay(true); err != nil {
		return wrap("tcp_no_delay_failed", err, "enable tcp no delay")
	}
	return nil
}

func (c *Client) handshake(ctx context.Context, conn net.Conn) error {
	req := protocol.HandshakeRequest{
		ClientID:     c.opts.ClientID,
		Tenant:       c.opts.Tenant,
		Namespace:    c.opts.Namespace,
		Principal:    c.opts.Principal,
		AuthToken:    c.opts.AuthToken,
		Capabilities: c.opts.Capabilities,
	}
	var out protocol.HandshakeResponse
	frame, err := c.newRequestFrame(protocol.CmdHandshakeRequest, req)
	if err != nil {
		return err
	}
	_, err = c.roundTripFrame(ctx, conn, frame, protocol.CmdHandshakeResponse, &out)
	if err != nil {
		return err
	}
	c.serverMu.Lock()
	c.server = out
	c.serverMu.Unlock()
	return nil
}

func closeConn(conn net.Conn) {
	if conn == nil {
		return
	}
	if err := conn.Close(); err != nil {
		return
	}
}
