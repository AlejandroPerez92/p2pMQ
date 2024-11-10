package main

import (
	"context"
	"fmt"
	"github.com/AlejandroPerez92/p2pMQ/examples/flag"
	config2 "github.com/AlejandroPerez92/p2pMQ/internal/config"
	"github.com/AlejandroPerez92/p2pMQ/internal/messaging"
	"github.com/AlejandroPerez92/p2pMQ/internal/node"
	"log"
)

func main() {
	ctx := context.Background()
	params := flag.ParseFlags()

	cfg := config2.ConsumerTopicConfig{
		TopicName:     "p2pmq-topic",
		ConsumerGroup: "test-1",
	}

	hostConfig := config2.HostConfig{Port: params.ListenPort}
	n, err := node.NewP2pMQNode(ctx, hostConfig)

	if err != nil {
		log.Fatal(err)
	}
	defer n.Host.Close()

	fmt.Println("p2pMQ node ID:", n.Host.ID().String())

	err = n.SubscribeToTopic(cfg, func(msg messaging.MqMessage) {
		fmt.Println(msg.Content)
		ackMessage := messaging.CreateAckMessage(msg)
		n.SendAck(ackMessage)
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}
