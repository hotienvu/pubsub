# Pubsub
A thread-safe lock-free pubsub library for Golang
```
p := pubsub.NewPubsub()
p.CreateTopic("topic")
s, err := p.Subscribe("topic")
err = p.Publish("topic", "message")
e := <- s.Events()
e.Cancel()
p.Close("topic")
p.CloseAll()
```
