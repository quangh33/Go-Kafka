# Go-Kafka: A Distributed, Fault-Tolerant Message Queue from Scratch
Go-Kafka is a from-scratch implementation of a distributed, replicated message queue inspired by Apache Kafka. This project is a deep dive into the core principles of distributed systems, demonstrating concepts like distributed consensus, replication, fault tolerance, and high-performance storage techniques, all built in pure Go.

It is not intended to be a production-ready replacement for Kafka, but rather a powerful demonstration of the engineering principles required to build such a system.

## Core Features
- **Replicated, Append-Only Commit Log**: At its heart is a durable, high-performance storage engine that writes messages to an immutable, append-only log file on disk. This log is the source of truth for all data.

- **Topics & Partitions**: The system supports organizing data into topics, which are subdivided into partitions. This allows for data isolation, multi-tenancy, and serves as the fundamental unit of parallelism for consumers.

- **Distributed Consensus with Raft**: The cluster uses the industry-standard Raft consensus algorithm (via hashicorp/raft) to ensure all brokers agree on the state of the data. This provides:

  - **Automatic Leader Election**: For each partition, one broker is elected leader to handle all writes.

  - **Log Replication**: The leader replicates all messages to follower nodes.

  - **Fault Tolerance**: The cluster can survive the failure of a minority of nodes without downtime or data loss. A new leader is automatically elected if the current one fails.

- **Cluster-Aware Client**: The client is designed to interact with a dynamic cluster. It features:

  - **Automatic Leader Discovery**: A client can connect to any node. If it contacts a follower, it will be automatically redirected to the correct leader.

- **Durable Consumer Offsets**: The broker tracks the progress of each consumer group, persisting their offsets to a file. This provides at-least-once message delivery guarantees, as a consumer can crash and restart without losing its place.

- **Configurable Producer Acknowledgements (acks)**: Producers can choose their desired level of durability on a per-message basis, demonstrating the classic trade-off between latency and safety:

  - `ack=all` (Default): The producer waits for the message to be durably replicated to a quorum of nodes. (Maximum safety)

  - `ack=none`: The producer sends the message and does not wait for a response. (Lowest latency)

# Getting Started: Running a 3-Node Cluster
## 1. Prerequisites
- Go 1.18+
- Protocol Buffers Compiler (protoc) and the Go gRPC plugins.

```bash
# Install Homebrew
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
# Install protobuf
brew install protobuf
# Install the Go Plugins
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
# Update your path
export PATH="$PATH:$(go env GOPATH)/bin"
```

## 2 Clear data
```bash
rm -rf ./data
```
## 3. Launch the Cluster
Run these commands in three separate terminals.

### Terminal 1: Start Node 1 (Initial Leader)
```bash
go run broker/main.go -id=node1 -grpc_addr=127.0.0.1:9092 -raft_addr=127.0.0.1:19092 -http_addr=127.0.0.1:8080
```
### Terminal 2: Start Node 2
```bash
go run broker/main.go -id=node2 -grpc_addr=127.0.0.1:9093 -raft_addr=127.0.0.1:19093 -http_addr=127.0.0.1:8081 -join_addr=127.0.0.1:8080
```
### Terminal 2: Start Node 3
```bash
go run broker/main.go -id=node3 -grpc_addr=127.0.0.1:9094 -raft_addr=127.0.0.1:19094 -http_addr=127.0.0.1:8082 -join_addr=127.0.0.1:8080
```

Wait 5 seconds for the cluster to stabilize.

## 5. Produce a message (terminal 4)
```bash
go run client/main.go produce 127.0.0.1:9092 replicated-topic 0 "first message"
```

## 6. Testing Fault Tolerance
### Stop Node 1
### Restart Node1 (rejoin cluster as a voter)
```bash
go run broker/main.go -id=node1 -grpc_addr=127.0.0.1:9092 -raft_addr=127.0.0.1:19092 -http_addr=127.0.0.1:8080 -join_addr=127.0.0.1:8081
```
### Produce another message to Node 1
```bash
go run client/main.go produce 127.0.0.1:9092 replicated-topic 0 "2nd message"
```

# Future Work
- [ ] Time-Based Log Retention
- [ ] Administrative APIs
- [ ] Consumer Group Rebalancing 
- [ ] "Exactly-once" delivery semantics
# License
This project is licensed under the MIT License.