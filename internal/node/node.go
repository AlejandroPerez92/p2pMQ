package node

import (
	"bufio"
	"context"
	"encoding/json"
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
	Host           host.Host
	PubSub         *pubsub.PubSub
	ReceivedQueues map[string]*messaging.ReceivedMessageQueue
	AckQueues      map[string]*pubsub.Topic
	SendQueues     map[string]*pubsub.Topic
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
		Host:           h,
		PubSub:         ps,
		ReceivedQueues: make(map[string]*messaging.ReceivedMessageQueue),
		AckQueues:      make(map[string]*pubsub.Topic),
		SendQueues:     make(map[string]*pubsub.Topic),
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

	queue, ok := n.ReceivedQueues[msg.Topic]
	if !ok {
		fmt.Println("Error queue not found", err)
		return
	}

	queue.AddMessage(msg)
}

func (n *P2pMQNode) SubscribeToTopic(ctx context.Context, config config.ConsumerTopicConfig, onMessage func(msg messaging.MqMessage)) error {

	ackTopicName := "ack." + config.TopicName + "." + config.ConsumerGroup
	topicProtocol := fmt.Sprintf("/direct-message/1.1.0/%s", config.TopicName)

	topic, err := n.PubSub.Join(config.TopicName)
	_, err = topic.Subscribe()
	if err != nil {
		return err
	}

	if err != nil {
		return err
	}
	n.ReceivedQueues[config.TopicName] = messaging.NewReceivedMessageQueue(topic, 1000)
	n.ReceivedQueues[config.TopicName].OnMessage(onMessage)

	ackTopic, err := n.PubSub.Join(ackTopicName)
	if err != nil {
		return err
	}
	n.AckQueues[ackTopicName] = ackTopic

	n.Host.SetStreamHandler(protocol.ID(topicProtocol), n.handleMessage)

	return nil
}

func (n *P2pMQNode) SendAck(ctx context.Context, topicName string, ackMsg messaging.MqMessage) error {
	ackMsgRaw, _ := json.Marshal(ackMsg)
	return n.AckQueues[topicName].Publish(ctx, ackMsgRaw)
}

func (n *P2pMQNode) PublishMessage(ctx context.Context, msg messaging.MqMessage) error {
	topic, ok := n.SendQueues[msg.Topic]

	if !ok {
		newTopic, err := n.PubSub.Join(msg.Topic)
		if err != nil {
			return err
		}
		n.SendQueues[msg.Topic] = newTopic
		topic = newTopic
	}

	var peerPtrs []*peer.ID
	for _, p := range topic.ListPeers() {
		peerPtrs = append(peerPtrs, &p)
	}

	if len(peerPtrs) == 0 {
		fmt.Println("No peers found")
		return nil
	}

	rr, _ := roundrobin.New(peerPtrs...)

	err := n.sendDirectMessage(ctx, *rr.Next(), msg)

	return err
}

func (n *P2pMQNode) sendDirectMessage(ctx context.Context, peerID peer.ID, message messaging.MqMessage) error {
	msgRaw, _ := json.Marshal(message)

	topicProtocol := fmt.Sprintf("/direct-message/1.1.0/%s", message.Topic)

	stream, err := n.Host.NewStream(ctx, peerID, protocol.ID(topicProtocol))
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
