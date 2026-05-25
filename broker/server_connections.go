package broker

import (
	"context"
	"net"
	"strings"
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/protocol"
	protocolbinary "github.com/lyonbrown4d/ech0/protocol/binary"
	"github.com/lyonbrown4d/ech0/transport"
)

type tcpConnectionIndex struct {
	mu         sync.Mutex
	clients    *collectionmapping.BiMap[string, string]
	identities *collectionmapping.Map[string, Identity]
}

func newTCPConnectionIndex() *tcpConnectionIndex {
	return &tcpConnectionIndex{
		clients:    collectionmapping.NewBiMap[string, string](),
		identities: collectionmapping.NewMap[string, Identity](),
	}
}

func (i *tcpConnectionIndex) Register(clientID, connectionID string, identity Identity) (Identity, bool) {
	clientID = strings.TrimSpace(clientID)
	connectionID = strings.TrimSpace(connectionID)
	if i == nil || connectionID == "" {
		return Identity{}, false
	}
	i.mu.Lock()
	previous, replaced := i.identities.Get(connectionID)
	if clientID != "" {
		i.clients.Put(clientID, connectionID)
	}
	i.identities.Set(connectionID, normalizeIdentity(identity))
	i.mu.Unlock()
	return previous, replaced
}

func (i *tcpConnectionIndex) DeleteConnection(connectionID string) (Identity, bool) {
	connectionID = strings.TrimSpace(connectionID)
	if i == nil || connectionID == "" {
		return Identity{}, false
	}
	i.mu.Lock()
	i.clients.DeleteByValue(connectionID)
	identity, deleted := i.identities.Get(connectionID)
	i.identities.Delete(connectionID)
	i.mu.Unlock()
	return identity, deleted
}

func (i *tcpConnectionIndex) ConnectionForClient(clientID string) (string, bool) {
	if i == nil {
		return "", false
	}
	i.mu.Lock()
	connectionID, ok := i.clients.GetByKey(clientID)
	i.mu.Unlock()
	return connectionID, ok
}

func (i *tcpConnectionIndex) ClientForConnection(connectionID string) (string, bool) {
	if i == nil {
		return "", false
	}
	i.mu.Lock()
	clientID, ok := i.clients.GetByValue(connectionID)
	i.mu.Unlock()
	return clientID, ok
}

func (i *tcpConnectionIndex) IdentityForConnection(connectionID string) (Identity, bool) {
	if i == nil {
		return Identity{}, false
	}
	i.mu.Lock()
	identity, ok := i.identities.Get(connectionID)
	i.mu.Unlock()
	return identity, ok
}

func (s *TCPServer) contextForConnectionFrame(ctx context.Context, conn net.Conn, frame transport.Frame) (context.Context, *transport.Frame) {
	if s == nil || s.clients == nil {
		return ctx, nil
	}
	connectionID := tcpConnectionID(conn)
	if frame.Header.Command == protocol.CmdHandshakeRequest {
		return s.contextFromHandshake(ctx, connectionID, frame)
	}
	identity, ok := s.clients.IdentityForConnection(connectionID)
	if !ok {
		return ctx, nil
	}
	return WithIdentity(ctx, identity), nil
}

func (s *TCPServer) contextFromHandshake(ctx context.Context, connectionID string, frame transport.Frame) (context.Context, *transport.Frame) {
	var req protocol.HandshakeRequest
	if err := protocolbinary.DecodeBody(frame.Header.Command, frame.Body, &req); err != nil {
		response := errorFrame("invalid_request", err.Error())
		return ctx, &response
	}
	identity, err := s.broker.authenticate(ctx, AuthRequest{
		ClientID:  req.ClientID,
		Principal: req.Principal,
		Tenant:    req.Tenant,
		Namespace: req.Namespace,
		Token:     req.AuthToken,
	})
	if err != nil {
		response := errorFromErr(err)
		return ctx, &response
	}
	previous, replaced := s.clients.IdentityForConnection(connectionID)
	if !replaced || quotaIdentityKey(previous) != quotaIdentityKey(identity) {
		if err := s.broker.quotaUsage.acquireConnection(ctx, s.broker, identity); err != nil {
			response := errorFromErr(err)
			return ctx, &response
		}
	}
	replacedIdentity, replaced := s.clients.Register(req.ClientID, connectionID, identity)
	if replaced && quotaIdentityKey(replacedIdentity) != quotaIdentityKey(identity) {
		s.broker.quotaUsage.releaseConnection(replacedIdentity)
	}
	return WithIdentity(ctx, identity), nil
}

func (s *TCPServer) unregisterConnection(conn net.Conn) {
	if s == nil || s.clients == nil {
		return
	}
	identity, ok := s.clients.DeleteConnection(tcpConnectionID(conn))
	if ok {
		s.broker.quotaUsage.releaseConnection(identity)
	}
}

func tcpConnectionID(conn net.Conn) string {
	if conn == nil || conn.RemoteAddr() == nil {
		return ""
	}
	return conn.RemoteAddr().String()
}
