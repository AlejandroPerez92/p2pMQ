package node

import (
	"github.com/AlejandroPerez92/p2pMQ/internal/messaging"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type MqTopic struct {
	MessageQueue            *messaging.MessageQueue
	AckTopic                *pubsub.Topic
	InfoConsumersTopic      *pubsub.Topic
	SendTopic               *pubsub.Topic
	ConsumerGreetingChannel *pubsub.Topic
}

func CreateForConsumer(
	queue *messaging.MessageQueue,
	consumersTopic *pubsub.Topic,
	ackTopic *pubsub.Topic,
	consumersGreetingChannel *pubsub.Topic,
) *MqTopic {
	return &MqTopic{
		MessageQueue:            queue,
		AckTopic:                ackTopic,
		InfoConsumersTopic:      consumersTopic,
		ConsumerGreetingChannel: consumersGreetingChannel,
	}
}

func CreateForProducer(
	ackTopic *pubsub.Topic,
	sendTopic *pubsub.Topic,
	consumersGreetingChannel *pubsub.Topic,
) *MqTopic {
	return &MqTopic{
		AckTopic:                ackTopic,
		SendTopic:               sendTopic,
		ConsumerGreetingChannel: consumersGreetingChannel,
	}
}
