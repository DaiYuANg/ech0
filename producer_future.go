package ech0

import (
	"context"
	"sync"
)

type ProduceFuture struct {
	done chan struct{}
	once sync.Once

	msg Message
	err error
}

func newProduceFuture() *ProduceFuture {
	return &ProduceFuture{done: make(chan struct{})}
}

func (f *ProduceFuture) Done() <-chan struct{} {
	if f == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return f.done
}

func (f *ProduceFuture) Await(ctx context.Context) (Message, error) {
	if f == nil {
		return Message{}, producerError("future_nil", "produce future is nil")
	}
	if ctx == nil {
		return Message{}, producerError("future_context_nil", "produce future context is nil")
	}
	select {
	case <-f.done:
		return f.msg, f.err
	case <-ctx.Done():
		return Message{}, producerWrap("future_await_context_done", ctx.Err(), "await produce future")
	}
}

func (f *ProduceFuture) complete(msg Message, err error) {
	f.once.Do(func() {
		f.msg = msg
		f.err = err
		close(f.done)
	})
}
