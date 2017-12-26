package pubsub

import (
	"errors"
)

type Pubsub interface {
	CreateTopic(topic string) error
	Subscribe(topic string) (Subscription, error)
	Publish(topic string, value interface{}) error
	Close(topic string) error
	CloseAll()
}

type pubsub struct {
	producers map[string]Producer
	subCmds chan SubscribeCmd
	createTopicCmds chan CreateTopicCmd
	pubCmds chan PublishCmd
	closeCmds chan CloseCmd
	closeAllCmds chan CloseAllCmd
}

type CreateTopicCmd struct {
	topic string
	res chan error
}

type SubscribeCmd struct {
	topic string
	res chan SubscribeResult
}

type SubscribeResult struct {
	sub Subscription
	err error
}

type PublishCmd struct {
	topic string
	value interface{}
	res chan error
}

type CloseCmd struct {
	topic string
	res chan error
}

type CloseAllCmd struct{}

func NewPubsub() Pubsub {
	p := &pubsub{
		producers: make(map[string]Producer),
		createTopicCmds: make(chan CreateTopicCmd),
		subCmds: make(chan SubscribeCmd),
		pubCmds: make(chan PublishCmd, 1000),
		closeCmds: make(chan CloseCmd),
		closeAllCmds: make(chan CloseAllCmd),
	}
	go func() {
		for {
			select {
			case c := <- p.createTopicCmds:
				c.res <- p.createTopic(c.topic)
			case c := <- p.subCmds:
				c.res <- p.subscribe(c.topic)
			case c := <- p.pubCmds:
				c.res <- p.publish(c.topic, c.value)
			case c := <- p.closeCmds:
				c.res <- p.close(c.topic)
			case <- p.closeAllCmds:
				p.closeAll()
			}
		}
	}()
	return p
}

func (p *pubsub)CreateTopic(topic string) error {
	res := make(chan error, 1)
	p.createTopicCmds <- CreateTopicCmd{ topic, res }
	return <- res
}

func (p *pubsub)createTopic(topic string) error {
	if _, ok := p.producers[topic]; ok {
		return errors.New("topic already existed: " + topic)
	} else {
		p.producers[topic] = NewProducer()
		return nil
	}
}

func (p *pubsub)Subscribe(topic string) (Subscription, error) {
	res := make(chan SubscribeResult, 1)
	p.subCmds <- SubscribeCmd{ topic, res }
	s := <- res
	return s.sub, s.err
}

func (p *pubsub)subscribe(topic string) SubscribeResult {
	if producer, ok := p.producers[topic]; ok {
		return SubscribeResult{ producer.Consume(), nil }
	} else {
		return SubscribeResult{ nil, errors.New("topic not found: " + topic)}
	}
}

func (p *pubsub)Publish(topic string, value interface{}) error {
	res := make(chan error, 1)
	p.pubCmds <- PublishCmd{ topic, value, res }
	return <- res
}

func (p *pubsub)publish(topic string, value interface{}) error {
	if producer, ok := p.producers[topic]; ok {
		producer.Input() <- value
		return nil
	} else {
		return errors.New("topic not found: " + topic)
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
		return errors.New("topic not found: " + topic)
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
