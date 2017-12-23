package pubsub

import (
	"strconv"
	"time"
	"math/rand"
)

type Producer interface {
	Consume() Subscription
	Shutdown()
	Input() chan <- interface{}
}


type producer struct {
	inputs chan interface{}
	subs map[string]*subscription
	newSubs chan *subscription
	subDone chan string
	done chan struct{}
}

func (p *producer) Input() chan <- interface{} {
	return p.inputs
}

func (p *producer) Shutdown() {
	close(p.done)
}

func (p *producer) Consume() Subscription {
	s := newSubscription()
	p.newSubs <- s
	go func() {
		<- s.done
		// clean up subscriber
		p.subDone <- s.id
	}()
	return s
}

func NewProducer() Producer {
	p := &producer{
		inputs: make(chan interface{}),
		subs: make(map[string]*subscription),
		subDone: make(chan string),
		done: make(chan struct{}),
		newSubs: make(chan *subscription),
	}
	go func() {
		for {
			select {
			case s := <- p.newSubs:
				p.subs[s.id] = s
			case m := <- p.inputs:
				for _, s := range p.subs {
					// we don't want to block here potentially,
					// so need to discard in case the channel is full
					// this only happens after subscriber cancel and
					// stop consuming message
					select {
					case s.events <- Event{ EventTypeNew, m}:
					default:
						// discarding event after subscriber cancel
					}
				}
			case <- p.done:
				for _, s := range p.subs {
					s.events <- Event{ EventTypeCompleted, nil}
					close(s.events)
					delete(p.subs, s.id)
				}
				return
			case sid := <- p.subDone:
				close(p.subs[sid].events)
				delete(p.subs, sid)
			}
		}
	}()
	return p
}

type Subscription interface {
	Events() <- chan Event
	Cancel()
}

type subscription struct {
	events chan Event
	done chan struct{}
	id string
}

func newSubscription() *subscription {
	s := &subscription{
		events: make(chan Event, 1000),
		done: make(chan struct{}),
		id: strconv.Itoa(time.Now().Nanosecond()) + strconv.Itoa(rand.Int()),
	}
	return s
}

func (s *subscription)Events() <- chan Event {
	return s.events
}

func (s *subscription)Cancel() {
	close(s.done)
}

type Event struct {
	Type EventType
	Payload interface{}
}

type EventType int

const (
	EventTypeNew EventType = iota
	EventTypeError
	EventTypeCompleted
)
