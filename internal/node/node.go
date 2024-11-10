package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlejandroPerez92/p2pMQ/internal/config"
	"github.com/AlejandroPerez92/p2pMQ/internal/messaging"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	roundrobin "github.com/thegeekyasian/round-robin-go"
	"io"
	"time"
)

type P2pMQNode struct {
	Host    host.Host
	pubSub  *pubsub.PubSub
	topics  map[string]*MqTopic // This map needs to be concurrent safe
	context context.Context
}

func NewP2pMQNode(
	ctx context.Context,
	config config.HostConfig,
) (*P2pMQNode, error) {

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1/", config.Port)),
	)

	if err != nil {
		return nil, err
	}

	if err := setupDiscovery(h); err != nil {
		panic(err)
	}

	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, err
	}

	return &P2pMQNode{
		Host:    h,
		pubSub:  ps,
		topics:  make(map[string]*MqTopic),
		context: ctx,
	}, nil
}

func (n *P2pMQNode) handleDirectMessage(stream network.Stream) {
	reader := bufio.NewReader(stream)

	msgBytes, err := io.ReadAll(reader)
	if err != nil {
		fmt.Println("Error reading stream:", err)
		return
	}

	var msg messaging.MqMessage
	err = json.Unmarshal(msgBytes, &msg)
	if err != nil {
		fmt.Println("Error unmarshaling message:", err)
		return
	}

	topic, ok := n.topics[msg.Topic]
	if !ok {
		fmt.Println("Error queue not found", err)
		return
	}

	topic.MessageQueue.AddMessage(msg)
}

func (n *P2pMQNode) SubscribeToTopic(config config.ConsumerTopicConfig, onMessage func(msg messaging.MqMessage)) error {
	topicProtocol := fmt.Sprintf("/direct-message/1.1.0/%s", config.TopicName)

	// this topic is only used for be discoverable by the producer because the messages are received by direct connection
	topic, err := n.pubSub.Join(config.TopicName)
	_, err = topic.Subscribe()
	if err != nil {
		return err
	}

	ackTopic, err := n.pubSub.Join("ack." + config.TopicName)
	if err != nil {
		return err
	}

	greetingTopic, err := n.pubSub.Join("greeting." + config.TopicName)
	if err != nil {
		return err
	}

	greeting := messaging.GreetingMessage{
		PeerId:        n.Host.ID().String(),
		ConsumerGroup: config.ConsumerGroup,
	}

	rawGreeting, err := json.Marshal(greeting)

	// needs to wait for the topic to be ready
	time.Sleep(1 * time.Second)
	err = greetingTopic.Publish(n.context, rawGreeting)

	if err != nil {
		return err
	}

	rcvQueue := messaging.NewReceivedMessageQueue(1000)
	rcvQueue.OnMessage(onMessage)

	n.Host.SetStreamHandler(protocol.ID(topicProtocol), n.handleDirectMessage)

	n.topics[config.TopicName] = CreateForConsumer(
		rcvQueue,
		topic,
		ackTopic,
		greetingTopic,
	)

	return nil
}

func (n *P2pMQNode) SendAck(ackMsg *messaging.AckMessage) error {
	topic, ok := n.topics[ackMsg.Topic]

	if !ok {
		return errors.New("topic not found")
	}

	ackMsgRaw, _ := json.Marshal(ackMsg)
	return topic.AckTopic.Publish(n.context, ackMsgRaw)
}

func (n *P2pMQNode) PublishMessage(msg messaging.MqMessage) error {
	topic, ok := n.topics[msg.Topic]

	if !ok {
		return errors.New("topic not initialized")
	}

	var peerPtrs []*peer.ID
	for _, p := range topic.SendTopic.ListPeers() {
		peerPtrs = append(peerPtrs, &p)
	}

	if len(peerPtrs) == 0 {
		fmt.Println("No peers found")
		return nil
	}

	rr, _ := roundrobin.New(peerPtrs...)

	return n.sendDirectMessage(*rr.Next(), msg)
}

func (n *P2pMQNode) sendDirectMessage(peerID peer.ID, message messaging.MqMessage) error {
	msgRaw, _ := json.Marshal(message)

	topicProtocol := fmt.Sprintf("/direct-message/1.1.0/%s", message.Topic)

	stream, err := n.Host.NewStream(n.context, peerID, protocol.ID(topicProtocol))
	if err != nil {
		return fmt.Errorf("error oppening stream: %w", err)
	}
	defer stream.Close()

	_, err = stream.Write(msgRaw)
	if err != nil {
		return fmt.Errorf("error sending direct message: %w", err)
	}

	fmt.Println("Message sent to peer:", peerID)
	return nil
}

func (n *P2pMQNode) InitializePublisher(topicName string) error {
	ackTopicName := "ack." + topicName

	ackTopic, err := n.pubSub.Join(ackTopicName)
	if err != nil {
		return err
	}

	ackSub, _ := ackTopic.Subscribe()

	greetingTopic, err := n.pubSub.Join("greeting." + topicName)
	if err != nil {
		return err
	}

	greetingSubs, err := greetingTopic.Subscribe()
	if err != nil {
		return err
	}

	fmt.Println("Subscribed to topic:", greetingTopic.String())
	newTopic, err := n.pubSub.Join(topicName)
	if err != nil {
		return err
	}

	topic := CreateForProducer(
		ackTopic,
		newTopic,
		greetingTopic,
	)

	n.topics[topicName] = topic

	go n.handleAckMessages(ackSub)
	go n.handleGreetingMessages(greetingSubs, topic)

	return nil
}

func (n *P2pMQNode) handleAckMessages(subs *pubsub.Subscription) {
	for {
		msg, err := subs.Next(n.context)
		if err != nil {
			fmt.Println("Error getting ack next message:", err)
		}

		var ackMsg messaging.AckMessage

		err = json.Unmarshal(msg.Data, &ackMsg)

		if err != nil {
			fmt.Println("Error unmarshalling ack message:", err)
		}

		// TODO Delete msg from event store
		fmt.Println("Ack message received: ", ackMsg.ID)
	}
}

func (n *P2pMQNode) handleGreetingMessages(subs *pubsub.Subscription, topic *MqTopic) {
	for {
		msg, err := subs.Next(n.context)
		if err != nil {
			fmt.Println("Error getting greeting next message:", err)
		}

		var greetingMessage messaging.GreetingMessage

		err = json.Unmarshal(msg.Data, &greetingMessage)

		if err != nil {
			fmt.Println("Error unmarshalling greeting message:", err)
		}

		// TODO Add peer to consumer group
		fmt.Println("Greeting message received: ", greetingMessage)
	}
}
