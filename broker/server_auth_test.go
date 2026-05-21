package broker_test

import (
	"context"
	"testing"

	broker "github.com/lyonbrown4d/ech0/broker"
	"github.com/lyonbrown4d/ech0/protocol"
	"github.com/lyonbrown4d/ech0/transport"
)

func TestTCPHandshakeUsesConfiguredStaticToken(t *testing.T) {
	cfg := broker.DefaultConfig()
	cfg.Governance.Auth.StaticTokens = []broker.StaticAuthTokenConfig{{
		Token:     "secret",
		Principal: "svc-a",
		Tenant:    "tenant-a",
		Namespace: "payments",
	}}
	b, err := broker.New(cfg)
	requireNoError(t, err)
	server := broker.NewTCPServer(cfg, b, nil, nil)

	resp := handleProtocolFrame[protocol.HandshakeResponse](context.Background(), t, server, protocol.CmdHandshakeRequest, protocol.CmdHandshakeResponse, protocol.HandshakeRequest{
		ClientID:  "client-1",
		AuthToken: "secret",
	})
	if resp.Principal != "svc-a" || resp.Tenant != "tenant-a" || resp.Namespace != "payments" {
		t.Fatalf("unexpected authenticated handshake identity: %#v", resp)
	}

	body, err := protocol.EncodeBody(protocol.CmdHandshakeRequest, protocol.HandshakeRequest{AuthToken: "bad"})
	requireNoError(t, err)
	frame, err := transport.NewFrame(protocol.Version, protocol.CmdHandshakeRequest, body)
	requireNoError(t, err)
	failed, err := server.HandleFrame(context.Background(), frame)
	requireNoError(t, err)
	if failed.Header.Command != protocol.CmdErrorResponse {
		t.Fatalf("expected invalid static token to fail handshake, got command %d", failed.Header.Command)
	}
}
