# p2pMQ

**p2pMQ** is a proof of concept for a peer-to-peer (P2P) message queue system built on top of [libp2p](https://libp2p.io/) using mDNS for service discovery.

This project aims to provide a robust messaging queue where peers can communicate without relying on a centralized server. By adopting a P2P approach, it can scale in line with the number of producers and consumers, avoiding the need for complex centralized infrastructure.

## How It Works 🛠️

The main idea is to keep the messaging system close to the code itself, so each replica of the code becomes a node in the P2P messaging network.

**p2pMQ** uses the built-in pub/sub feature from [libp2p](https://libp2p.io/) to discover which nodes are consumers of a given topic. Once the target nodes are discovered, it establishes direct communication to send messages to specific nodes. This approach ensures that each message is consumed only once.

A consumer group feature is currently under development. With this feature, each message will be consumed by only one consumer within each consumer group.

> *A consumer group is a single logical consumer implemented with multiple physical consumers for reasons of throughput and resilience.*

## Try It Out! 😎

### Install Dependencies
Run the following command to install dependencies:
```shell
go mod tidy
```

### Run a Producer 🏭
Open a terminal and run the producer. Type a message and press Enter to send it.
```shell
go run examples/consume-produce/consumer.go -port 6660
```

### Run a Consumer 📥
Open another terminal to run a consumer. You can run multiple consumers by specifying different ports for each instance.
```shell
go run examples/consume-produce/consumer.go -port 6661
```

Once the system is running, each message will be sent to a single consumer chosen at random.