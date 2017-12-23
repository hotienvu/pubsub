package pubsub

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestPubsub_Publish(t *testing.T) {
	p := NewPubsub()
	s := p.Subscribe("test")
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
	s1 := p.Subscribe("test")
	s2 := p.Subscribe("test")
	p.Publish("test", "foo")
	e1 := <- s1.Events()
	e2 := <- s2.Events()
	assert.Equal(t, e1.Payload, "foo")
	assert.Equal(t, e1.Payload, e2.Payload)
}

func TestPubsub_MultipleTopics(t *testing.T) {
	p := NewPubsub()
	s1 := p.Subscribe("foo")
	s2 := p.Subscribe("bar")
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
	N := 200
	M := 1000
	K := len(topics)
	for i:=0;i<N;i++ {
		// consumer
		go func(idx int) {
			s := p.Subscribe(topics[idx % K])
			for _ = range s.Events() {
			}
		}(i)
	}
	//producer
	for i:=0;i<K;i++ {
		go func(idx int) {
			for j := 0;j<M;j++ {
				p.Publish(topics[idx], j)
			}
		}(i)
	}
	p.CloseAll()
}