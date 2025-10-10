package main

import (
	"Go-Kafka/api"
	"Go-Kafka/storage"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
)

// server implements the gRPC server for our Kafka service.
type server struct {
	api.UnimplementedKafkaServer

	dataDir string
	mu      sync.RWMutex
	logs    map[string]*storage.CommitLog // map from topic-partition to its log
}

// newServer creates a new gRPC server instance.
func newServer(dataDir string) (*server, error) {
	return &server{
		dataDir: dataDir,
		logs:    make(map[string]*storage.CommitLog),
	}, nil
}

// getOrCreateLog retrieves the commit log for a given topic and partition.
// It creates the log and its directories if they don't exist.
func (s *server) getOrCreateLog(topic string, partition uint32) (*storage.CommitLog, error) {
	// The key for our map will be topic-partition
	logIdentifier := fmt.Sprintf("%s-%d", topic, partition)

	// First, check with a read lock for performance
	s.mu.RLock()
	cl, ok := s.logs[logIdentifier]
	s.mu.RUnlock()
	if ok {
		return cl, nil
	}

	// If not found, acquire a write lock to create it
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check in case another goroutine created it while we were waiting for the lock
	cl, ok = s.logs[logIdentifier]
	if ok {
		return cl, nil
	}

	// Create the log file path
	topicDir := filepath.Join(s.dataDir, topic)
	if err := os.MkdirAll(topicDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create topic directory: %w", err)
	}
	logPath := filepath.Join(topicDir, fmt.Sprintf("%d.log", partition))

	// Create a new CommitLog instance
	cl, err := storage.NewCommitLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("could not create commit log: %w", err)
	}

	// Store it in our map
	s.logs[logIdentifier] = cl
	return cl, nil
}

// Produce handles a produce request from a client.
// Produce handles a produce request from a client.
func (s *server) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	cl, err := s.getOrCreateLog(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}
	offset, err := cl.Append(req.Value)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// getLog retrieves the commit log for a given topic and partition.
// It returns an error if the log does not exist.
func (s *server) getLog(topic string, partition uint32) (*storage.CommitLog, error) {
	logIdentifier := fmt.Sprintf("%s-%d", topic, partition)
	s.mu.RLock()
	defer s.mu.RUnlock()
	cl, ok := s.logs[logIdentifier]
	if !ok {
		// We could define a more specific gRPC error status here if we wanted.
		return nil, fmt.Errorf("topic %s partition %d not found", topic, partition)
	}
	return cl, nil
}

// Consume handles a consume request from a client.
func (s *server) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	cl, err := s.getLog(req.Topic, req.Partition)
	if err != nil {
		return nil, err
	}
	record, nextOffset, err := cl.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{
		Record: &api.Record{
			Value:  record,
			Offset: nextOffset,
		},
	}, nil
}

func main() {
	grpcAddr := flag.String("addr", "127.0.0.1:9092", "Address for gRPC server")
	dataDir := flag.String("data_dir", "./data", "Directory for logs")
	flag.Parse()

	log.Printf("Starting Go-Kafka broker on %s", *grpcAddr)

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}
	logPath := filepath.Join(*dataDir, "commit.log")

	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv, err := newServer(logPath)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}

	api.RegisterKafkaServer(grpcServer, srv)

	log.Printf("Broker listening on %s", *grpcAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
