package config

type ConsumerTopicConfig struct {
	TopicName     string
	ConsumerGroup string
}

type HostConfig struct {
	Port int
}
