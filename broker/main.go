package main

import (
	"Go-Kafka/api"
	"Go-Kafka/cluster"
	"Go-Kafka/storage"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// NodeMetadata holds the public addresses for a node.
type NodeMetadata struct {
	GRPCAddr string
}

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

	metadataMu sync.RWMutex
	metadata   map[string]NodeMetadata // map of node IDs to their metadata

	raft *raft.Raft
}

// newServer creates a new gRPC server instance.
func newServer(dataDir string) (*server, error) {
	srv := &server{
		dataDir:     dataDir,
		logs:        make(map[string]*storage.CommitLog),
		offsets:     make(map[string]int64),
		offsetsPath: filepath.Join(dataDir, "offsets.json"),
		metadata:    make(map[string]NodeMetadata),
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
func (s *server) GetOrCreateLog(topic string, partition uint32) (*storage.CommitLog, error) {
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
func (s *server) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	// In Raft, all changes to the system's state (like producing a new message) must go through the leader.
	// This ensures that all changes are put into a single, globally agreed-upon order.
	// If a client mistakenly sends a Produce request to a follower, this code rejects the request and helpfully
	// provides the address of the current leader so the client can retry.
	if s.raft.State() != raft.Leader {
		_, leaderID := s.raft.LeaderWithID()
		s.metadataMu.RLock()
		leaderMeta, ok := s.metadata[string(leaderID)]
		s.metadataMu.RUnlock()
		log.Printf("leaderId %s", leaderID)
		log.Printf("metadata: %v", s.metadata)
		if !ok {
			return nil, fmt.Errorf("leader gRPC address not found in metadata")
		}
		return &api.ProduceResponse{
			ErrorCode:  api.ErrorCode_NOT_LEADER,
			LeaderAddr: leaderMeta.GRPCAddr,
		}, nil
	}

	payload := cluster.ProduceCommandPayload{
		Topic:     req.Topic,
		Partition: req.Partition,
		Value:     req.Value,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	cmd := cluster.Command{
		Type:    cluster.ProduceCommand,
		Payload: payloadBytes,
	}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}

	// 1. The leader writes the command to its own internal Raft log.
	// 2. It sends the command to all follower nodes.
	// 3. It waits until a majority of nodes in the cluster have safely saved the command to their logs.
	// 4. Only then does the applyFuture.Error() call return without an error.
	// This guarantees the message is durably replicated before we ever respond to the client.
	applyFuture := s.raft.Apply(cmdBytes, 5*time.Second)

	if req.Ack == api.AckLevel_NONE {
		// Fire-and-forget. Return immediately without waiting for Raft to commit.
		return &api.ProduceResponse{ErrorCode: api.ErrorCode_OK}, nil
	}

	// For acks=ALL, we wait for the command to be committed by a quorum.
	if err := applyFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to apply raft command: %w", err)
	}

	response, ok := applyFuture.Response().(cluster.ApplyResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected FSM response type")
	}

	return &api.ProduceResponse{Offset: response.Offset, ErrorCode: api.ErrorCode_OK}, nil
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

// resolveAdvertisableAddr resolves a potentially non-advertisable bind address
// into an advertisable address that can be shared with other nodes.
func resolveAdvertisableAddr(addr string) (net.Addr, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}

	// If the address is wildcard (0.0.0.0 or :port), we need to find a specific IP.
	if tcpAddr.IP.IsUnspecified() {
		addrs, err := net.InterfaceAddrs()
		if err != nil {
			return nil, err
		}
		for _, address := range addrs {
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					// Found a non-loopback IPv4 address, use it.
					return &net.TCPAddr{
						IP:   ipnet.IP,
						Port: tcpAddr.Port,
					}, nil
				}
			}
		}
		// If no suitable non-loopback IP is found, default to localhost for local testing.
		return &net.TCPAddr{
			IP:   net.ParseIP("127.0.0.1"),
			Port: tcpAddr.Port,
		}, nil
	}
	return tcpAddr, nil
}

// UpdateMetadata is called by the FSM to update the server's in-memory metadata map.
// This method makes the server struct satisfy the cluster.StateManager interface.
func (s *server) UpdateMetadata(nodeID, grpcAddr string) {
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	log.Printf("Update metadata, nodeId %s, grpcAddr %s", nodeID, grpcAddr)
	s.metadata[nodeID] = NodeMetadata{
		GRPCAddr: grpcAddr,
	}
}

// setupRaft initializes and returns a Raft instance.
func setupRaft(nodeID, raftAddr, dataDir string, srv *server) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	// Sets the unique identifier for this specific node within the Raft cluster.
	// The nodeID is the string we passed in from the command-line flags (e.g., "node1").
	// It's converted to the raft.ServerID type.
	// Raft needs a unique ID for every node to know who is voting and who is sending messages.
	// This ID must be unique across the entire cluster.
	config.LocalID = raft.ServerID(nodeID)

	// Ensures the provided network address is valid before trying to use it.
	addr, err := resolveAdvertisableAddr(raftAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve advertisable address: %w", err)
	}
	// Creates the network layer that Raft nodes will use to communicate with each other.
	// It opens a TCP listener on raftAddr and configures it.
	// Raft is a distributed protocol; nodes must be able to send RPCs (like votes and log entries) to each other.
	// This transport handles all that low-level networking
	transport, err := raft.NewTCPTransport(raftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft transport: %w", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(dataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot store: %w", err)
	}

	// Creates the persistent storage for the Raft log.
	// It uses the raft-boltdb library to create and manage a file named raft.db inside the node's data directory.
	// The log of commands agreed upon by the cluster must survive crashes and restarts.
	// This file stores that log durably on disk.
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("failed to create bolt store: %w", err)
	}
	// Creates an instance of our custom Finite State Machine (FSM).
	// The FSM is the bridge between Raft's log and our application's state (the CommitLog files).
	fsm := cluster.NewFSM(srv)

	ra, err := raft.NewRaft(config, fsm, logStore, logStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft: %w", err)
	}
	return ra, nil
}

// bootstrapCluster handles the logic of either starting a new cluster or joining an existing one.
func bootstrapCluster(nodeID, raftAddr, grpcAddr, joinAddr string, ra *raft.Raft) error {
	bootstrap := joinAddr == ""
	// bootstrap == true means we are going to start a new cluster
	if bootstrap {
		log.Println("Bootstrapping new cluster")
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(nodeID),
					Address: raft.ServerAddress(raftAddr),
				},
			},
		}
		bootstrapFuture := ra.BootstrapCluster(configuration)
		if err := bootstrapFuture.Error(); err != nil {
			return fmt.Errorf("failed to bootstrap cluster: %w", err)
		}

		log.Println("Waiting for the node to become the leader...")
		select {
		case <-ra.LeaderCh():
			log.Println("Node has become the leader.")
		case <-time.After(10 * time.Second):
			return fmt.Errorf("timed out waiting for leadership")
		}

		// replicate leader metadata to Raft log
		payload := cluster.UpdateMetadataPayload{NodeID: nodeID, GRPCAddr: grpcAddr}
		payloadBytes, _ := json.Marshal(payload)
		cmd := cluster.Command{Type: cluster.UpdateMetadataCommand, Payload: payloadBytes}
		cmdBytes, _ := json.Marshal(cmd)
		applyFuture := ra.Apply(cmdBytes, 5*time.Second)
		if err := applyFuture.Error(); err != nil {
			return fmt.Errorf("failed to apply initial metadata: %w", err)
		}
		log.Println("Bootstrapped cluster successfully")
		return nil
	}

	log.Printf("Attempting to join existing cluster at %s", joinAddr)
	currentJoinAddr := joinAddr
	leaderAddrRegex := regexp.MustCompile(`([0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]+`)
	// Joining might not succeed on the first try (e.g., the leader might be busy or a network blip might occur)
	// The loop ensures the node is persistent and will keep retrying
	for {
		time.Sleep(1 * time.Second)

		conn, err := grpc.NewClient(currentJoinAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("failed to connect to join address, retrying: %v", err)
			continue
		}
		client := api.NewKafkaClient(conn)
		// "Hello, node at {joinAddr}, my name is {nodeID}, my Raft address is {raftAddr},
		// and I would like to join the cluster."
		_, err = client.Join(context.Background(), &api.JoinRequest{
			NodeId:   nodeID,
			RaftAddr: raftAddr,
			GrpcAddr: grpcAddr,
		})
		conn.Close()

		if err == nil {
			log.Println("Successfully joined cluster")
			return nil
		}

		if strings.Contains(err.Error(), "not the leader") {
			matches := leaderAddrRegex.FindStringSubmatch(err.Error())
			if len(matches) > 0 {
				newLeaderAddr := matches[0]
				log.Printf("Join failed: target node is not the leader. Redirecting to new leader at %s", newLeaderAddr)
				currentJoinAddr = newLeaderAddr
				continue
			}
			log.Printf("Join failed: target node is not the leader, but could not parse leader hint. Retrying original address... (%v)", err)
			continue
		}
		log.Printf("failed to join cluster with address %s, retrying: %v", currentJoinAddr, err)
	}
}

// Join handles a request from a new node to join the cluster.
func (s *server) Join(ctx context.Context, req *api.JoinRequest) (*api.JoinResponse, error) {
	if s.raft.State() != raft.Leader {
		_, leaderID := s.raft.LeaderWithID()
		if leaderID == "" {
			return nil, fmt.Errorf("not the leader, and no leader is known")
		}
		s.metadataMu.RLock()
		leaderGRPCAddr, ok := s.metadata[string(leaderID)]
		s.metadataMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("not the leader, and leader metadata not yet available for leader ID %s", leaderID)
		}
		return nil, fmt.Errorf("not the leader, current leader is at %s", leaderGRPCAddr)
	}

	// tell the Raft consensus layer about the new node. It adds the new node's ID and its private Raft address to the cluster's configuration
	addPeerFuture := s.raft.AddVoter(raft.ServerID(req.NodeId), raft.ServerAddress(req.RaftAddr), 0, 5*time.Second)
	if err := addPeerFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to add peer as voter: %w", err)
	}

	payload := cluster.UpdateMetadataPayload{NodeID: req.NodeId, GRPCAddr: req.GrpcAddr}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata payload: %w", err)
	}
	cmd := cluster.Command{Type: cluster.UpdateMetadataCommand, Payload: payloadBytes}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata command: %w", err)
	}
	// tell the application layer on all nodes about the new node's public-facing gRPC address.
	applyFuture := s.raft.Apply(cmdBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to apply metadata command: %w", err)
	}

	log.Printf("Successfully added node %s to the cluster", req.NodeId)
	return &api.JoinResponse{}, nil
}

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
	srv, err := newServer(dataDir)
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
	ra, err := setupRaft(*nodeID, *raftAddr, dataDir, srv)
	if err != nil {
		log.Fatalf("failed to setup raft: %v", err)
	}
	srv.raft = ra
	log.Println("Finish setting up Raft!")
	// --- Cluster Bootstrapping ---
	log.Println("Starting cluster bootstrap/join process...")
	if err := bootstrapCluster(*nodeID, *raftAddr, *grpcAddr, *joinAddr, ra); err != nil {
		log.Fatalf("failed to bootstrap or join cluster: %v", err)
	}

	// Block forever
	select {}
}
