package messaging

type GreetingMessage struct {
	PeerId        string `json:"peerId"`
	ConsumerGroup string `json:"consumerGroup"`
}
