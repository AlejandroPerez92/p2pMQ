package messaging

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type ReceivedMessageQueue struct {
	messages chan MqMessage
	Topic    *pubsub.Topic
}

func NewReceivedMessageQueue(topic *pubsub.Topic, bufferSize int) *ReceivedMessageQueue {
	return &ReceivedMessageQueue{
		messages: make(chan MqMessage, bufferSize),
		Topic:    topic,
	}
}

func (mq *ReceivedMessageQueue) AddMessage(msg MqMessage) {
	mq.messages <- msg
}

func (mq *ReceivedMessageQueue) OnMessage(callback func(msg MqMessage)) {
	go func() {
		for msg := range mq.messages {
			callback(msg)
		}
	}()
}
