package messaging

import (
	"time"
)

type AckMessage struct {
	ID      string    `json:"id"`
	AckedAt time.Time `json:"ackedAt"`
	Topic   string    `json:"topic"`
}

func CreateAckMessage(msg MqMessage) *AckMessage {
	return &AckMessage{
		ID:      msg.ID,
		AckedAt: time.Time{},
		Topic:   msg.Topic,
	}
}

func (msg *AckMessage) AckTopic() string {
	return "ack." + msg.Topic
}
