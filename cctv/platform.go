package cctv

import (
	"fmt"
	"slices"
	"sync"
)

type Platform struct {
	rw   sync.RWMutex
	subs map[Topic][]Subscriber
}

func NewPlatform() *Platform {
	return &Platform{
		rw:   sync.RWMutex{},
		subs: map[Topic][]Subscriber{},
	}
}

// Publish some content of topic, only subscriptions to that specific topic will get notified.
func (p *Platform) Publish(topic Topic, content Content) error {
	p.rw.RLock()
	subs, ok := p.subs[topic]
	p.rw.RUnlock()

	if !ok {
		return fmt.Errorf("No dispatcher for topic(%s)", topic)
	}

	for _, sub := range subs {
		sub.dispatch(content)
	}

	return nil
}

// Creates a subscription with a sender chan.
// This function takes ownership of ch, and closes ch on unsubsription. So user should not use ch later.
func (p *Platform) SubscribeWithChan(topic Topic, ch chan<- Content) Subscriber {
	sub := &subscriberLoss{ch: ch, topic: topic, platform: p}

	p.rw.Lock()
	defer p.rw.Unlock()

	subs, ok := p.subs[topic]
	if !ok {
		subs = make([]Subscriber, 0, 64)
	}
	subs = append(subs, sub)
	p.subs[topic] = subs

	return sub
}

// Creates a subscription with config.
// Returned channel will be closed on unsubscription.
func (p *Platform) SubscribeWithConfig(topic Topic, cfg SubConfig) (Subscriber, <-chan Content) {
	ch := make(chan Content, cfg.BufferSize)

	var sub Subscriber
	if cfg.NoLoss {
		if cfg.CanBlock {
			sub = &subscriberBlock{ch: ch, topic: topic, platform: p}
		} else {
			sub = &subscriberNonBlock{ch: ch, topic: topic, platform: p}
		}
	} else {
		sub = &subscriberLoss{ch: ch, topic: topic, platform: p}
	}

	subs, ok := p.subs[topic]
	if !ok {
		subs = make([]Subscriber, 0, 64)
	}
	subs = append(subs, sub)
	p.subs[topic] = subs

	return sub, ch
}

func (p *Platform) unsubscribe(topic Topic, sub Subscriber) {
	p.rw.Lock()
	defer p.rw.Unlock()

	subs, ok := p.subs[topic]
	if !ok {
		return
	}

	p.subs[topic] = slices.DeleteFunc(subs, func(s Subscriber) bool { return sub == s })
}
