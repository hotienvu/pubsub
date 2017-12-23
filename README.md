# Pubsub
A thread-safe lock-free pubsub library for Golang
```
p := pubsub.NewPubsub()
s := p.Subscribe("topic")
p.Publish("topic", "message")
e := <- s.Events()
e.Cancel()
p.Close("topic")
```
