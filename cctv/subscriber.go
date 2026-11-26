package cctv

import (
	"sync"
)

// By default, a Subscriber is lossy and zero-buffered
type SubConfig struct {
	NoLoss     bool // NoLoss indicates a no-loss subscriber, which will not discard content if buffer is full
	CanBlock   bool // (NoLoss)CanStop indicates a blockable subscriber, calling dispatch() of which may block
	BufferSize int  // BufferSize indicates the underlying buffer size
}

type Subscriber interface {
	Unsubscribe()

	dispatch(Content)
}

// A loss subscriber discards contents if the buffer is full
type subscriberLoss struct {
	ch       chan<- Content
	platform *Platform
	topic    Topic

	mu     sync.RWMutex
	closed bool
}

func (sub *subscriberLoss) dispatch(content Content) {
	sub.mu.RLock()
	defer sub.mu.RUnlock()
	if sub.closed {
		return
	}

	select {
	case sub.ch <- content:
	default:
		// Drop the content
	}
}

func (sub *subscriberLoss) Unsubscribe() {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	if sub.closed {
		return
	}
	sub.closed = true
	sub.platform.unsubscribe(sub.topic, sub)
	close(sub.ch)
}

type subscriberBlock struct {
	ch       chan<- Content
	platform *Platform
	topic    Topic

	mu     sync.RWMutex
	closed bool
}

func (sub *subscriberBlock) dispatch(content Content) {
	sub.mu.RLock()
	defer sub.mu.RUnlock()

	sub.ch <- content
}

func (sub *subscriberBlock) Unsubscribe() {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	if sub.closed {
		return
	}
	sub.closed = true
	sub.platform.unsubscribe(sub.topic, sub)
	close(sub.ch)
}

type subscriberNonBlock struct {
	ch       chan<- Content
	platform *Platform
	topic    Topic

	mu     sync.RWMutex
	closed bool
}

func (sub *subscriberNonBlock) dispatch(content Content) {
	go func() {
		sub.mu.RLock()
		defer sub.mu.RUnlock()

		sub.ch <- content
	}()
}

func (sub *subscriberNonBlock) Unsubscribe() {
	sub.mu.Lock()
	defer sub.mu.Unlock()

	if sub.closed {
		return
	}
	sub.closed = true
	sub.platform.unsubscribe(sub.topic, sub)
	close(sub.ch)
}
