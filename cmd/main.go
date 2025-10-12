package main

import (
	"Go-Kafka/api"
	"Go-Kafka/broker"
	"flag"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"path/filepath"
)

func main() {
	nodeID := flag.String("id", "", "Node ID")
	// grpcAddr is the main address that your clients (producers and consumers) connect to.
	// Analogy: This is the public phone number
	grpcAddr := flag.String("grpc_addr", "127.0.0.1:9092", "Address for gRPC server")
	// raftAddr is the address that broker nodes use to talk to each other for all internal Raft consensus business.
	// Analogy: This is the internal, private extension number used for communication between employees within the company
	raftAddr := flag.String("raft_addr", "127.0.0.1:19092", "Address for Raft server")
	// joinAddr is the network address of another, already existing node in the cluster
	// A new node uses this address to find the existing cluster and ask to be added as a member.
	joinAddr := flag.String("join_addr", "", "Address to join an existing Raft cluster")
	dataDirRoot := flag.String("data_dir", "./data", "Directory for logs")
	flag.Parse()

	if *nodeID == "" {
		log.Fatal("node ID is required")
	}

	dataDir := filepath.Join(*dataDirRoot, *nodeID)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("failed to create data dir: %v", err)
	}

	// --- Start gRPC Server ---
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv, err := broker.NewServer(dataDir)
	if err != nil {
		log.Fatalf("failed to create server: %v", err)
	}
	api.RegisterKafkaServer(grpcServer, srv)
	log.Printf("Broker listening on %s", *grpcAddr)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve gRPC: %v", err)
		}
	}()

	// --- Setup Raft ---
	log.Println("Start setting up Raft...")
	err = srv.SetupRaft(*nodeID, *raftAddr, dataDir, srv)
	if err != nil {
		log.Fatalf("failed to setup raft: %v", err)
	}
	log.Println("Finish setting up Raft!")
	// --- Cluster Bootstrapping ---
	log.Println("Starting cluster bootstrap/join process...")
	if err := srv.BootstrapCluster(*nodeID, *raftAddr, *grpcAddr, *joinAddr); err != nil {
		log.Fatalf("failed to bootstrap or join cluster: %v", err)
	}

	// Block forever
	select {}
}
