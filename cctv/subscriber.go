package cctv

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

type subscriberLoss struct {
	ch       chan<- Content
	platform *Platform
	topic    Topic
}

func (sub *subscriberLoss) dispatch(content Content) {
	select {
	case sub.ch <- content:
	default:
		// Drop the content
	}
}

func (sub *subscriberLoss) Unsubscribe() {
	sub.platform.unsubscribe(sub.topic, sub)
	close(sub.ch)
}

type subscriberBlock struct {
	ch       chan<- Content
	platform *Platform
	topic    Topic
}

func (sub *subscriberBlock) dispatch(content Content) {
	sub.ch <- content
}
func (sub *subscriberBlock) Unsubscribe() {
	sub.platform.unsubscribe(sub.topic, sub)
	close(sub.ch)
}

type subscriberNonBlock struct {
	ch       chan<- Content
	platform *Platform
	topic    Topic
}

func (sub *subscriberNonBlock) dispatch(content Content) {
	go func() { sub.ch <- content }()
}

func (sub *subscriberNonBlock) Unsubscribe() {
	sub.platform.unsubscribe(sub.topic, sub)
	close(sub.ch)
}
