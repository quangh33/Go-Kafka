package cluster

import (
	"Go-Kafka/storage"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"log"
)

// The command type for producing a message.
type CommandType string

const (
	ProduceCommand        CommandType = "PRODUCE"
	UpdateMetadataCommand CommandType = "UPDATE_METADATA"
)

// ProduceCommandPayload is the data that gets written to the Raft log.
type ProduceCommandPayload struct {
	Topic     string
	Partition uint32
	Value     []byte
}

// UpdateMetadataPayload is the data for updating node metadata.
type UpdateMetadataPayload struct {
	NodeID   string
	GRPCAddr string
}

// ApplyResponse is the response from the FSM after applying a command.
type ApplyResponse struct {
	Offset int64
}

// Command represents a command to be applied to the FSM.
type Command struct {
	Type    CommandType
	Payload []byte
}

// StateManager is an interface that the server must implement to allow the FSM
// to interact with its state in a decoupled way.
type StateManager interface {
	GetOrCreateLog(topic string, partition uint32) (*storage.CommitLog, error)
	UpdateMetadata(nodeID, grpcAddr string)
}

// fsm is the Raft Finite State Machine. It applies commands from the Raft log
// to the actual data store (our commit logs).
type fsm struct {
	state StateManager
}

// NewFSM creates a new FSM.
func NewFSM(sm StateManager) *fsm {
	return &fsm{state: sm}
}

// Apply applies a Raft log entry to the FSM.
// This method is called by the hashicorp/raft library automatically whenever a log entry is committed by the cluster.
// the Raft library guarantees that this Apply method will be called with the same sequence of logs on every single
// server. By executing these steps, every server will independently but identically update its on-disk CommitLog files,
// ensuring they stay perfectly in sync.
func (f *fsm) Apply(logEntry *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(logEntry.Data, &cmd); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch cmd.Type {
	case ProduceCommand:
		var payload ProduceCommandPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			panic(fmt.Sprintf("failed to unmarshal produce payload: %s", err.Error()))
		}

		commitLog, err := f.state.GetOrCreateLog(payload.Topic, payload.Partition)
		if err != nil {
			panic(fmt.Sprintf("failed to get or create log: %s", err.Error()))
		}
		offset, err := commitLog.Append(payload.Value)
		if err != nil {
			panic(fmt.Sprintf("failed to append to commit log: %s", err.Error()))
		}
		return ApplyResponse{Offset: offset}
	case UpdateMetadataCommand:
		var payload UpdateMetadataPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			panic(fmt.Sprintf("failed to unmarshal metadata payload: %s", err))
		}
		f.state.UpdateMetadata(payload.NodeID, payload.GRPCAddr)
		log.Printf("Replicated metadata update for node %s -> %s", payload.NodeID, payload.GRPCAddr)
		return nil
	default:
		panic(fmt.Sprintf("unrecognized command type: %d", cmd.Type))
	}
}

// Snapshot returns a snapshot of the current state. For this project, we
// don't implement snapshots, but the interface requires it.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

// Restore restores the FSM to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return nil
}

type snapshot struct{}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *snapshot) Release() {}
