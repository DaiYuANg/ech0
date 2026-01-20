package broker

import (
	"encoding/json"
	"errors"
	"net"
	"sync"

	"github.com/DaiYuANg/ech0/internal/protocol"
)

var ErrTopicNotFound = errors.New("topic not found")

type Broker struct {
	mu       sync.RWMutex
	topics   map[string][]net.Conn
	messages map[string][]json.RawMessage
}

func NewBroker() *Broker {
	return &Broker{
		topics:   make(map[string][]net.Conn),
		messages: make(map[string][]json.RawMessage),
	}
}

// Broker 不关心 Transport 是 TCP / WebSocket，只管分发
func (b *Broker) Subscribe(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics[topic] = append(b.topics[topic], conn)
}

func (b *Broker) Publish(topic string, payload json.RawMessage) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	conns := append([]net.Conn{}, b.topics[topic]...)
	for _, c := range conns {
		env := protocol.Envelope{
			Api:     "message",
			Topic:   topic,
			Payload: payload,
		}
		data, _ := env.Encode()
		data = append(data, '\n')
		c.Write(data)
	}
}
