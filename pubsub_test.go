package pubsub

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"time"
)

func TestPubsub_SubscribeNonExistentTopic(t *testing.T) {
	p := NewPubsub()
	s, e := p.Subscribe("test")
	assert.NotNil(t, e)
	assert.Nil(t, s)
}

func TestPubsub_PublishNonExistentTopic(t *testing.T) {
	p := NewPubsub()
	e := p.Publish("test", "foo")
	assert.NotNil(t, e)
}

func TestPubsub_PublishSubscribe(t *testing.T) {
	p := NewPubsub()
	err := p.CreateTopic("test")
	s, err := p.Subscribe("test")
	assert.Nil(t, err)
	p.Publish("test", "foo")
	p.Publish("test", "bar")
	e := <- s.Events()
	assert.Equal(t, e.Type, EventTypeNew, "subscriber should receive new message")
	assert.Equal(t, e.Payload, "foo", "subscriber should receive correct message")
	e = <- s.Events()
	assert.Equal(t, e.Payload, "bar", "message should receive multiple messages")
}

func TestPubsub_MultipleSubscriber(t *testing.T) {
	p := NewPubsub()
	p.CreateTopic("test")
	s1, _ := p.Subscribe("test")
	s2, _ := p.Subscribe("test")
	p.Publish("test", "foo")
	e1 := <- s1.Events()
	e2 := <- s2.Events()
	assert.Equal(t, e1.Payload, "foo")
	assert.Equal(t, e1.Payload, e2.Payload)
}

func TestPubsub_MultipleTopics(t *testing.T) {
	p := NewPubsub()
	p.CreateTopic("foo")
	p.CreateTopic("bar")
	s1, _ := p.Subscribe("foo")
	s2, _ := p.Subscribe("bar")
	p.Publish("foo", "a")
	p.Publish("bar", "b")
	e1 := <- s1.Events()
	e2 := <- s2.Events()
	assert.Equal(t, e1.Payload, "a")
	assert.Equal(t, e2.Payload, "b")
}

func TestRacePubsub(t *testing.T) {
	p := NewPubsub()
	topics := []string { "a", "b", "c", "d"}
	// create topics thread
	go func() {
		for _, t := range topics {
			p.CreateTopic(t)
		}
	}()
	// destroy topics thread
	go func() {
		for _, t := range topics {
			p.Close(t)
		}
	}()

	N := 200
	M := 1000
	K := len(topics)
	for i:=0;i<N;i++ {
		// consumer threads
		go func(idx int) {
			for {
				s, e := p.Subscribe(topics[idx % K])
				if e == nil {
					for _ = range s.Events() {
					}
				}
			}
		}(i)
	}
	//producer threads
	for i:=0;i<K;i++ {
		go func(idx int) {
			for j := 0;j<M;j++ {
				p.Publish(topics[idx], j)
			}
		}(i)
	}
	// let this run for a while
	time.Sleep(time.Second)
}