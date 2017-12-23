package pubsub

import (
	"testing"
	"sync"
	"math/rand"
	"github.com/stretchr/testify/assert"
)

func TestRaceConsumer(t *testing.T) {
	p := NewProducer()
	N := 1000

	wg := sync.WaitGroup{}
	wg.Add(N)
	for i:=0;i<N;i++ {
		s := p.Consume()
		go func() {
			for e := range s.Events() {
				cancel := rand.Intn(100) < 1
				if cancel {
					s.Cancel()
					wg.Done()
					return
				}
				if e.Type == EventTypeCompleted  {
					wg.Done()
					return
				}
			}
		}()
	}
	for i :=0;i<1000000;i++ {
		p.Input() <- "data"
	}
	p.Shutdown()
	wg.Wait()
}

func TestConsumer(t *testing.T) {
	p := NewProducer()
	c := p.Consume()
	p.Input() <- "foo"
	p.Input() <- "bar"
	e := <- c.Events()
	assert.Equal(t, e.Payload, "foo")
}
