package pubsub

import (
	"errors"
)

type Pubsub interface {
	Subscribe(topic string) Subscription
	Publish(topic string, value interface{})
	Close(topic string) error
	CloseAll()
}

type pubsub struct {
	producers map[string]Producer
	subCmds chan SubscribeCmd
	pubCmds chan PublishCmd
	closeCmds chan CloseCmd
	closeAllCmds chan CloseAllCmd
}

type SubscribeCmd struct {
	topic string
	res chan Subscription
}

type PublishCmd struct {
	topic string
	value interface{}
}

type CloseCmd struct {
	topic string
	res chan error
}

type CloseAllCmd struct{}

func NewPubsub() Pubsub {
	p := &pubsub{
		producers: make(map[string]Producer),
		subCmds: make(chan SubscribeCmd),
		pubCmds: make(chan PublishCmd),
		closeCmds: make(chan CloseCmd),
		closeAllCmds: make(chan CloseAllCmd),
	}
	go func() {
		for {
			select {
			case c := <- p.subCmds:
				c.res <- p.subscribe(c.topic)
			case c := <- p.pubCmds:
				p.publish(c.topic, c.value)
			case c := <- p.closeCmds:
				c.res <- p.close(c.topic)
			case <- p.closeAllCmds:
				p.closeAll()
			}
		}
	}()
	return p
}

func (p *pubsub)Subscribe(topic string) Subscription {
	res := make(chan Subscription, 1)
	p.subCmds <- SubscribeCmd{ topic, res }
	return <- res
}

func (p *pubsub)subscribe(topic string) Subscription {
	if producer, ok := p.producers[topic]; ok {
		return producer.Consume()
	} else {
		p.producers[topic] = NewProducer()
		return p.producers[topic].Consume()
	}
}

func (p *pubsub)Publish(topic string, value interface{})  {
	p.pubCmds <- PublishCmd{ topic, value }
}

func (p *pubsub)publish(topic string, value interface{}) {
	if producer, ok := p.producers[topic]; ok {
		producer.Input() <- value
	} else {
		p.producers[topic] = NewProducer()
		p.producers[topic].Input() <- value
	}
}

func (p *pubsub)Close(topic string) error {
	res := make(chan error, 1)
	p.closeCmds <- CloseCmd{ topic, res }
	return <- res
}

func (p *pubsub)close(topic string) error {
	if producer, ok := p.producers[topic]; ok {
		producer.Shutdown()
		return nil
	} else {
		return errors.New("topic doesn't exit")
	}
}

func (p *pubsub)CloseAll() {
	p.closeAllCmds <- CloseAllCmd{}
}

func (p *pubsub)closeAll() {
	for topic, producer := range p.producers {
		producer.Shutdown()
		delete(p.producers, topic)
	}
}
