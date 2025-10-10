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
## Terminal 1
- Start Broker
```bash
go run broker/main.go
```
## Terminal 2
- Produce messages
```bash
go run client/main.go produce "hello world"
go run client/main.go produce "go-kafka is awesome"
```
- Consume message
```bash
go run client/main.go consume 0
go run client/main.go consume 19
```