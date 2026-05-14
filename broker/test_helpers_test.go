package broker_test

import (
	"context"
	"net"
	"testing"
	"time"

	broker "github.com/lyonbrown4d/ech0/broker"
)

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	listenConfig := net.ListenConfig{}
	listener, err := listenConfig.Listen(context.Background(), "tcp", "127.0.0.1:0")
	requireNoError(t, err)
	defer func() {
		if err := listener.Close(); err != nil {
			t.Logf("close listener: %v", err)
		}
	}()
	return listener.Addr().String()
}

func waitForLeader(t *testing.T, b *broker.Broker) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		health := b.RuntimeHealth()
		if health.Raft != nil && health.Raft.LocalIsLeader {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("raft leader not elected: %#v", b.RuntimeHealth())
}

func waitForAnyLeader(t *testing.T, brokers []*broker.Broker) *broker.Broker {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, b := range brokers {
			if b == nil {
				continue
			}
			health := b.RuntimeHealth()
			if health.Raft != nil && health.Raft.LocalIsLeader {
				return b
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("raft cluster leader not elected")
	return nil
}

func stopBroker(t *testing.T, b *broker.Broker) {
	t.Helper()
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	requireNoError(t, b.Stop(stopCtx))
}
