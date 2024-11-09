package node

import (
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"p2pmq/internal/messaging"
)

type MqTopic struct {
	MessageQueue       *messaging.MessageQueue
	AckTopic           *pubsub.Topic
	InfoConsumersTopic *pubsub.Topic
	SendTopic          *pubsub.Topic
}

func CreateForConsumer(
	queue *messaging.MessageQueue,
	consumersTopic *pubsub.Topic,
	ackTopic *pubsub.Topic,
) *MqTopic {
	return &MqTopic{
		MessageQueue:       queue,
		AckTopic:           ackTopic,
		InfoConsumersTopic: consumersTopic,
	}
}

func CreateForProducer(
	ackTopic *pubsub.Topic,
	sendTopic *pubsub.Topic,
) *MqTopic {
	return &MqTopic{
		AckTopic:  ackTopic,
		SendTopic: sendTopic,
	}
}
