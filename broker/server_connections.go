package broker

import (
	"net"
	"strings"
	"sync"

	collectionmapping "github.com/arcgolabs/collectionx/mapping"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/transport"
)

type tcpConnectionIndex struct {
	mu      sync.Mutex
	clients *collectionmapping.BiMap[string, string]
}

func newTCPConnectionIndex() *tcpConnectionIndex {
	return &tcpConnectionIndex{clients: collectionmapping.NewBiMap[string, string]()}
}

func (i *tcpConnectionIndex) Register(clientID, connectionID string) {
	clientID = strings.TrimSpace(clientID)
	connectionID = strings.TrimSpace(connectionID)
	if i == nil || clientID == "" || connectionID == "" {
		return
	}
	i.mu.Lock()
	i.clients.Put(clientID, connectionID)
	i.mu.Unlock()
}

func (i *tcpConnectionIndex) DeleteConnection(connectionID string) bool {
	connectionID = strings.TrimSpace(connectionID)
	if i == nil || connectionID == "" {
		return false
	}
	i.mu.Lock()
	deleted := i.clients.DeleteByValue(connectionID)
	i.mu.Unlock()
	return deleted
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

func (s *TCPServer) registerConnectionHandshake(conn net.Conn, frame transport.Frame) {
	if s == nil || s.clients == nil || frame.Header.Command != protocol.CmdHandshakeRequest {
		return
	}
	var req protocol.HandshakeRequest
	if err := protocol.DecodeBody(frame.Header.Command, frame.Body, &req); err != nil {
		return
	}
	s.clients.Register(req.ClientID, tcpConnectionID(conn))
}

func (s *TCPServer) unregisterConnection(conn net.Conn) {
	if s == nil || s.clients == nil {
		return
	}
	s.clients.DeleteConnection(tcpConnectionID(conn))
}

func tcpConnectionID(conn net.Conn) string {
	if conn == nil || conn.RemoteAddr() == nil {
		return ""
	}
	return conn.RemoteAddr().String()
}
