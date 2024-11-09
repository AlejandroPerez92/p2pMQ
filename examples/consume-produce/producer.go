package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/google/uuid"
	"log"
	"os"
	"p2pmq/examples/flag"
	config2 "p2pmq/internal/config"
	"p2pmq/internal/messaging"
	"p2pmq/internal/node"
	"time"
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

	scanner := bufio.NewScanner(os.Stdin)
	fmt.Println("Write something and press enter, press CTRL+C to finish")

	for scanner.Scan() {
		input := scanner.Text()
		err := n.PublishMessage(ctx, messaging.MqMessage{
			ID:          uuid.New().String(),
			Content:     input,
			PublishedAt: time.Now(),
			Topic:       cfg.TopicName,
		})

		if err != nil {
			fmt.Println("Error publishing message", err)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading scanner", err)
	}

	select {}
}