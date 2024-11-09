package node

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	roundrobin "github.com/thegeekyasian/round-robin-go"
	"io"
	"p2pmq/internal/config"
	"p2pmq/internal/messaging"
)

type P2pMQNode struct {
	Host    host.Host
	pubSub  *pubsub.PubSub
	topics  map[string]*MqTopic
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

func (n *P2pMQNode) handleMessage(stream network.Stream) {
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

	rcvQueue := messaging.NewReceivedMessageQueue(1000)
	rcvQueue.OnMessage(onMessage)

	n.Host.SetStreamHandler(protocol.ID(topicProtocol), n.handleMessage)

	n.topics[config.TopicName] = CreateForConsumer(rcvQueue, topic, ackTopic)

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
		newTopic, err := n.initializePublishTopic(msg.Topic)
		if err != nil {
			return err
		}
		n.topics[msg.Topic] = newTopic
		topic = newTopic
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

func (n *P2pMQNode) initializePublishTopic(topic string) (*MqTopic, error) {
	ackTopicName := "ack." + topic

	ackTopic, err := n.pubSub.Join(ackTopicName)
	if err != nil {
		return nil, err
	}

	sub, _ := ackTopic.Subscribe()

	go n.handleAckMessages(sub)

	newTopic, err := n.pubSub.Join(topic)
	if err != nil {
		return nil, err
	}

	return CreateForProducer(
		ackTopic,
		newTopic,
	), nil
}

func (n *P2pMQNode) handleAckMessages(sub *pubsub.Subscription) {
	for {
		msg, err := sub.Next(n.context)
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
