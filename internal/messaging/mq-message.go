package messaging

import (
	"github.com/google/uuid"
	"time"
)

type MqMessage struct {
	ID          string    `json:"id"`          // ID único del mensaje
	Content     string    `json:"content"`     // Contenido del mensaje
	PublishedAt time.Time `json:"publishedAt"` // Marca de tiempo de creación
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
