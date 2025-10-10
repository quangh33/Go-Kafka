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
)

// server implements the gRPC server for our Kafka service.
type server struct {
	api.UnimplementedKafkaServer
	commitLog *storage.CommitLog
}

// newServer creates a new gRPC server instance.
func newServer(logPath string) (*server, error) {
	cl, err := storage.NewCommitLog(logPath)
	if err != nil {
		return nil, fmt.Errorf("could not create commit log: %w", err)
	}
	return &server{commitLog: cl}, nil
}

// Produce handles a produce request from a client.
func (s *server) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.commitLog.Append(req.Value)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

// Consume handles a consume request from a client.
func (s *server) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, nextOffset, err := s.commitLog.Read(req.Offset)
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
