# Install
1. Install Homebrew
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

2. Install Protobuf
```bash
brew install protobuf
```

3. Install the Go Plugins
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

4. Update your path
```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

# Test
1. Clear data
```bash
rm -rf ./data
```
2. Start leader node (node1)
```bash
go run broker/main.go -id=node1 -grpc_addr=127.0.0.1:9092 -raft_addr=127.0.0.1:19092 -http_addr=127.0.0.1:8080
```
3. Start node2
```bash
go run broker/main.go -id=node2 -grpc_addr=127.0.0.1:9093 -raft_addr=127.0.0.1:19093 -http_addr=127.0.0.1:8081 -join_addr=127.0.0.1:8080
```
4. Start node3
```bash
go run broker/main.go -id=node3 -grpc_addr=127.0.0.1:9094 -raft_addr=127.0.0.1:19094 -http_addr=127.0.0.1:8082 -join_addr=127.0.0.1:8080
```
5. Produce a message
```bash
go run client/main.go produce 127.0.0.1:9092 replicated-topic 0 "first message"
```
6. Stop node1
7. Restart node1 (rejoin cluster as a voter)
```bash
go run broker/main.go -id=node1 -grpc_addr=127.0.0.1:9092 -raft_addr=127.0.0.1:19092 -http_addr=127.0.0.1:8080 -join_addr=127.0.0.1:8081
```
8. Produce another message to node1
```bash
go run client/main.go produce 127.0.0.1:9092 replicated-topic 0 "2nd message"
```

