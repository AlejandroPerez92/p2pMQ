package messaging

import (
	"github.com/google/uuid"
	"time"
)

type MqMessage struct {
	ID          string    `json:"id"`
	Content     string    `json:"content"`
	PublishedAt time.Time `json:"publishedAt"`
	Topic       string    `json:"topic"`
}

func CreateMessage(content string, topic string) *MqMessage {
	return &MqMessage{
		ID:          uuid.New().String(),
		Content:     content,
		PublishedAt: time.Now(),
		Topic:       topic,
	}
}
