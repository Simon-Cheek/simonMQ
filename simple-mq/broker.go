package main

type Broker struct {
	queues map[string]*Queue
}

type Queue struct {
	name     string
	messages []*QueueMsg
}

type QueueMsg struct {
	MsgId   string
	Payload string
}

func (b *Broker) enqueue(name string, payload string) *QueueMsg {

	msg := &QueueMsg{
		MsgId:   "",
		Payload: payload,
	}
	return msg
}
