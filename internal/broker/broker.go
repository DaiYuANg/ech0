package broker

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"path"
	"sync"

	"github.com/DaiYuANg/ech0/internal/protocol"
	"github.com/dgraph-io/badger/v4"
	"github.com/goradd/maps"
)

var ErrTopicNotFound = errors.New("topic not found")

type Broker struct {
	mu       sync.RWMutex
	topics   *maps.SafeMap[string, []net.Conn]
	messages *maps.SafeMap[string, []json.RawMessage]
	db       *badger.DB
}

func NewBroker() (*Broker, error) {
	os.TempDir()
	db, err := badger.Open(badger.DefaultOptions(path.Join(os.TempDir(), "ech0")))
	if err != nil {
		return nil, err
	}
	return &Broker{
		topics:   maps.NewSafeMap[string, []net.Conn](),
		messages: maps.NewSafeMap[string, []json.RawMessage](),
		db:       db,
	}, nil
}

// Broker 不关心 Transport 是 TCP / WebSocket，只管分发
func (b *Broker) Subscribe(topic string, conn net.Conn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	v, _ := b.topics.Load(topic)
	v = append(v, conn)
	b.topics.Set(topic, v)
}

func (b *Broker) Publish(topic string, payload json.RawMessage) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	conns := append([]net.Conn{}, b.topics.Get(topic)...)
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
