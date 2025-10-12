package broker

import (
	"Go-Kafka/api"
	"Go-Kafka/cluster"
	"context"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"time"
)

func (s *server) CreateTopic(ctx context.Context, req *api.CreateTopicRequest) (*api.CreateTopicResponse, error) {
	if s.raft.State() != raft.Leader {
		return nil, fmt.Errorf("this node is not the cluster leader")
	}

	s.topicMu.RLock()
	_, ok := s.topicMetadata[req.Topic]
	s.topicMu.RUnlock()
	if ok {
		return &api.CreateTopicResponse{ErrorCode: api.ErrorCode_TOPIC_ALREADY_EXISTS}, nil
	}

	payload := cluster.CreateTopicPayload{Name: req.Topic, Partitions: req.Partitions}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	cmd := cluster.Command{Type: cluster.CreateTopicCommand, Payload: payloadBytes}
	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	applyFuture := s.raft.Apply(cmdBytes, 5*time.Second)
	if err := applyFuture.Error(); err != nil {
		return nil, fmt.Errorf("failed to apply create topic command: %w", err)
	}
	return &api.CreateTopicResponse{ErrorCode: api.ErrorCode_OK}, nil
}
