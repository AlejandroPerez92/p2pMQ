package messaging

type MessageQueue struct {
	messages chan MqMessage
}

func NewReceivedMessageQueue(bufferSize int) *MessageQueue {
	return &MessageQueue{
		messages: make(chan MqMessage, bufferSize),
	}
}

func (mq *MessageQueue) AddMessage(msg MqMessage) {
	mq.messages <- msg
}

func (mq *MessageQueue) OnMessage(callback func(msg MqMessage)) {
	go func() {
		for msg := range mq.messages {
			callback(msg)
		}
	}()
}
