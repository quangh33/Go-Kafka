package main

import (
	"Go-Kafka/api"
	"Go-Kafka/storage"
	"context"
	"encoding/json"
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

	// For managing commit logs
	logMu sync.RWMutex
	logs  map[string]*storage.CommitLog // map from topic-partition to its log
	// For managing consumer group offsets
	offsetMu    sync.RWMutex
	offsets     map[string]int64 // Key: <group_id>-<topic>-<partition>, Value: offset
	offsetsPath string           // Path to the durable offsets file
}

// newServer creates a new gRPC server instance.
func newServer(dataDir string) (*server, error) {
	srv := &server{
		dataDir:     dataDir,
		logs:        make(map[string]*storage.CommitLog),
		offsets:     make(map[string]int64),
		offsetsPath: filepath.Join(dataDir, "offsets.json"),
	}

	// Load offsets from the durable file on startup
	if err := srv.loadOffsets(); err != nil {
		return nil, fmt.Errorf("failed to load offsets: %w", err)
	}

	return srv, nil
}

// loadOffsets reads the offsets file from disk into the in-memory map.
func (s *server) loadOffsets() error {
	data, err := os.ReadFile(s.offsetsPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("offsets.json not found, starting with empty offsets.")
			return nil // No offsets file yet, that's fine
		}
		return err
	}
	s.offsetMu.Lock()
	defer s.offsetMu.Unlock()
	return json.Unmarshal(data, &s.offsets)
}

// persistOffsets writes the current in-memory offset map to the JSON file.
func (s *server) persistOffsets() error {
	s.offsetMu.RLock()
	defer s.offsetMu.RUnlock()
	data, err := json.Marshal(s.offsets)
	if err != nil {
		return err
	}
	// WriteFile is atomic for simple cases
	return os.WriteFile(s.offsetsPath, data, 0644)
}

// getOrCreateLog retrieves the commit log for a given topic and partition.
// It creates the log and its directories if they don't exist.
func (s *server) getOrCreateLog(topic string, partition uint32) (*storage.CommitLog, error) {
	// The key for our map will be topic-partition
	logIdentifier := fmt.Sprintf("%s-%d", topic, partition)

	// First, check with a read lock for performance
	s.logMu.RLock()
	cl, ok := s.logs[logIdentifier]
	s.logMu.RUnlock()
	if ok {
		return cl, nil
	}

	// If not found, acquire a write lock to create it
	s.logMu.Lock()
	defer s.logMu.Unlock()

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
	s.logMu.RLock()
	defer s.logMu.RUnlock()
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

// CommitOffset handles a request from a consumer to save its progress.
func (s *server) CommitOffset(ctx context.Context, req *api.CommitOffsetRequest) (*api.CommitOffsetResponse, error) {
	offsetIdentifier := fmt.Sprintf("%s-%s-%d", req.ConsumerGroupId, req.Topic, req.Partition)
	s.offsetMu.Lock()
	s.offsets[offsetIdentifier] = req.Offset
	s.offsetMu.Unlock()

	// Persist the offsets to disk after updating in memory
	if err := s.persistOffsets(); err != nil {
		log.Printf("ERROR: failed to persist offsets: %v", err)
		// In a real system, you might want to return an error to the client here
	}

	log.Printf("Committed offset %d for group %s, topic %s, partition %d", req.Offset, req.ConsumerGroupId, req.Topic, req.Partition)
	return &api.CommitOffsetResponse{}, nil
}

// FetchOffset handles a request from a consumer to retrieve its last saved progress.
func (s *server) FetchOffset(ctx context.Context, req *api.FetchOffsetRequest) (*api.FetchOffsetResponse, error) {
	offsetIdentifier := fmt.Sprintf("%s-%s-%d", req.ConsumerGroupId, req.Topic, req.Partition)
	s.offsetMu.RLock()
	defer s.offsetMu.RUnlock()
	offset, ok := s.offsets[offsetIdentifier]
	if !ok {
		// If no offset is stored for this group, they start at the beginning.
		return &api.FetchOffsetResponse{Offset: 0}, nil
	}
	return &api.FetchOffsetResponse{Offset: offset}, nil
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
