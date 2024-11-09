package main

import (
	"context"
	"fmt"
	"log"
	"p2pmq/examples/flag"
	config2 "p2pmq/internal/config"
	"p2pmq/internal/messaging"
	"p2pmq/internal/node"
)

func main() {
	ctx := context.Background()
	params := flag.ParseFlags()

	cfg := config2.ConsumerTopicConfig{
		TopicName: "p2pmq-topic",
	}

	hostConfig := config2.HostConfig{Port: params.ListenPort}
	n, err := node.NewP2pMQNode(ctx, hostConfig)

	if err != nil {
		log.Fatal(err)
	}
	defer n.Host.Close()

	fmt.Println("p2pMQ node ID:", n.Host.ID().String())

	err = n.SubscribeToTopic(ctx, cfg, func(msg messaging.MqMessage) {
		fmt.Println(msg.Content)
		n.SendAck(ctx)
	})

	if err != nil {
		log.Fatal(err)
	}

	select {}
}
