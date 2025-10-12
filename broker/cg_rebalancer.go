// APIs related to consumer group rebalancing
package broker

import (
	"Go-Kafka/api"
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	"log"
	"time"
)

// heartbeatChecker is a background goroutine that checks for dead consumers.
func (s *server) heartbeatChecker() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.coordinatorMu.Lock()
		for _, group := range s.consumerGroups {
			group.mu.Lock()
			// It checks if the consumer group is currently in the middle of a rebalance (PreparingRebalance).
			// If it is, the checker completely skips that group and moves on to the next one.
			// This is a critical safety feature to prevent a race condition. When a consumer is participating in a rebalance,
			// it has to stop its regular work (including sending heartbeats) to make the JoinGroup and SyncGroup calls.
			// Without this check, the heartbeatChecker would see the lack of heartbeats,
			// incorrectly assume the consumer has crashed, and try to trigger another rebalance on top of the one
			// already in progress, leading to chaos.
			if group.State == RebalanceInProgress {
				group.mu.Unlock()
				continue
			}
			for consumerID, member := range group.Members {
				if time.Since(member.LastHeartbeat) > sessionTimeout {
					log.Printf("Consumer %s in group %s timed out. Triggering rebalance.", consumerID, group.GroupId)
					delete(group.Members, consumerID)
					s.startRebalance(group)
					break
				}
			}
			group.mu.Unlock()
		}
		s.coordinatorMu.Unlock()
	}
}

// startRebalance begins a new rebalance generation.
func (s *server) startRebalance(group *ConsumerGroup) {
	if group.State == RebalanceInProgress {
		return // Rebalance already in progress.
	}
	log.Printf("Group %s state changed to RebalanceInProgress", group.GroupId)
	group.State = RebalanceInProgress
	group.generationID++
	group.waitingJoin = make(map[string]chan *api.JoinGroupResponse)
	group.waitingSync = make(map[string]chan *api.SyncGroupResponse)

	// Start a timer. When it fires, the join phase is over.
	if group.joinTimer != nil {
		group.joinTimer.Stop()
	}
	group.joinTimer = time.AfterFunc(rebalanceTimeout, func() {
		s.finishJoinPhase(group)
	})
}

// finishJoinPhase is called when the rebalance timer expires.
func (s *server) finishJoinPhase(group *ConsumerGroup) {
	group.mu.Lock()
	defer group.mu.Unlock()

	// Handle the case where a rebalance ends with an empty group.
	// If there are no members left after the join window, the rebalance is over
	// and the group should become stable again.
	if len(group.Members) == 0 {
		log.Printf("Group %s is now empty, transitioning to Stable state.", group.GroupId)
		group.State = Stable
		group.waitingJoin = nil
		return
	}

	if len(group.waitingJoin) == 0 {
		return // No one was waiting.
	}

	log.Printf("Finishing join phase for group %s generation %d", group.GroupId, group.generationID)

	var leaderID string
	for id := range group.Members {
		leaderID = id // Elect first member as leader.
		break
	}

	// Send responses to all waiting consumers.
	for consumerID, respChan := range group.waitingJoin {
		resp := &api.JoinGroupResponse{LeaderId: leaderID}
		if consumerID == leaderID {
			resp.IsLeader = true
			resp.Members = make(map[string]*api.TopicMetadata)
			for id := range group.Members {
				resp.Members[id] = &api.TopicMetadata{}
			}
			resp.Partitions = group.Partitions
		}
		respChan <- resp
	}
	group.waitingJoin = nil // Clear the waiting map.
}

// finishSyncPhase distributes the assignment to all waiting members.
func (s *server) finishSyncPhase(group *ConsumerGroup) {
	log.Printf("Finishing sync phase for group %s generation %d", group.GroupId, group.generationID)

	for consumerID, respChan := range group.waitingSync {
		assignedPartitions := group.Assignments[consumerID]
		respChan <- &api.SyncGroupResponse{
			Assignment: &api.PartitionAssignment{Partitions: assignedPartitions},
		}
	}
	group.waitingSync = nil
	group.State = Stable
	log.Printf("Group %s rebalance complete. New state: Stable", group.GroupId)
}

// JoinGroup handles a consumer's request to join a group.
func (s *server) JoinGroup(ctx context.Context, req *api.JoinGroupRequest) (*api.JoinGroupResponse, error) {
	if s.raft.State() != raft.Leader {
		_, leaderID := s.raft.LeaderWithID()
		s.metadataMu.RLock()
		leaderGRPCAddr, ok := s.metadata[string(leaderID)]
		s.metadataMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("leader gRPC address not found in metadata")
		}

		return nil, fmt.Errorf("not the leader, current leader is at %s", leaderGRPCAddr)
	}

	s.topicMu.RLock()
	topicMeta, topicExists := s.topicMetadata[req.Topic]
	s.topicMu.RUnlock()
	if !topicExists {
		return nil, fmt.Errorf("topic %s not found", req.Topic)
	}
	partitions := make([]uint32, topicMeta.Partitions)
	for i := range partitions {
		partitions[i] = uint32(i)
	}

	s.coordinatorMu.Lock()
	group, ok := s.consumerGroups[req.GroupId]
	if !ok {
		group = &ConsumerGroup{
			GroupId:    req.GroupId,
			State:      Stable,
			Members:    make(map[string]*GroupMember),
			Topic:      req.Topic,
			Partitions: partitions,
		}
		s.consumerGroups[req.GroupId] = group
	}
	s.coordinatorMu.Unlock()

	group.mu.Lock()
	if _, ok := group.Members[req.ConsumerId]; !ok {
		log.Printf("New member %s joining group %s.", req.ConsumerId, req.GroupId)
		group.Members[req.ConsumerId] = &GroupMember{ConsumerID: req.ConsumerId}
		s.startRebalance(group)
	}
	member := group.Members[req.ConsumerId]
	member.LastHeartbeat = time.Now()

	if group.State != RebalanceInProgress {
		group.mu.Unlock()
		return &api.JoinGroupResponse{ErrorCode: api.ErrorCode_REBALANCE_IN_PROGRESS}, nil
	}

	// Create a response channel, add it to the waiting map, and wait.
	respChan := make(chan *api.JoinGroupResponse, 1)
	group.waitingJoin[req.ConsumerId] = respChan
	group.mu.Unlock()

	// Wait for the response from the finishJoinPhase function.
	select {
	case resp := <-respChan:
		return resp, nil
	case <-time.After(rebalanceTimeout + 1*time.Second):
		return nil, fmt.Errorf("timed out waiting for join phase to complete")
	}
}

// SyncGroup handles receiving the assignment from the leader and distributing it.
// It ensures that the broker (the Group Coordinator) will not distribute any partition assignments to any consumer
// until it has received a SyncGroup request from every single member of the group
func (s *server) SyncGroup(ctx context.Context, req *api.SyncGroupRequest) (*api.SyncGroupResponse, error) {
	if s.raft.State() != raft.Leader {
		_, leaderID := s.raft.LeaderWithID()
		s.metadataMu.RLock()
		leaderGRPCAddr, ok := s.metadata[string(leaderID)]
		s.metadataMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("leader gRPC address not found in metadata")
		}

		return nil, fmt.Errorf("not the leader, current leader is at %s", leaderGRPCAddr)
	}
	s.coordinatorMu.Lock()
	group, ok := s.consumerGroups[req.GroupId]
	if !ok {
		s.coordinatorMu.Unlock()
		return &api.SyncGroupResponse{ErrorCode: api.ErrorCode_UNKNOWN_MEMBER_ID}, nil
	}
	s.coordinatorMu.Unlock()

	group.mu.Lock()

	if group.State != RebalanceInProgress {
		group.mu.Unlock()
		return nil, fmt.Errorf("group is not rebalancing")
	}

	if len(req.Assignments) > 0 { // This is the leader's sync request
		group.Assignments = make(map[string][]uint32)
		for consumerID, assignment := range req.Assignments {
			group.Assignments[consumerID] = assignment.Partitions
		}
	}

	respChan := make(chan *api.SyncGroupResponse, 1)
	group.waitingSync[req.ConsumerId] = respChan

	// If all members have now synced, finish the rebalance.
	if len(group.waitingSync) == len(group.Members) {
		s.finishSyncPhase(group)
	}

	group.mu.Unlock()

	// Wait for the response from finishSyncPhase.
	select {
	case resp := <-respChan:
		return resp, nil
	case <-time.After(rebalanceTimeout + 1*time.Second):
		return nil, fmt.Errorf("timed out waiting for sync phase to complete")
	}
}

// Heartbeat handles a consumer's heartbeat, keeping it in the group.
func (s *server) Heartbeat(ctx context.Context, req *api.HeartbeatRequest) (*api.HeartbeatResponse, error) {
	if s.raft.State() != raft.Leader {
		_, leaderID := s.raft.LeaderWithID()
		s.metadataMu.RLock()
		leaderGRPCAddr, ok := s.metadata[string(leaderID)]
		s.metadataMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("leader gRPC address not found in metadata")
		}

		return nil, fmt.Errorf("not the leader, current leader is at %s", leaderGRPCAddr)
	}
	s.coordinatorMu.Lock()
	group, ok := s.consumerGroups[req.GroupId]
	if !ok {
		s.coordinatorMu.Unlock()
		return &api.HeartbeatResponse{ErrorCode: api.ErrorCode_UNKNOWN_MEMBER_ID}, nil
	}
	s.coordinatorMu.Unlock()

	group.mu.Lock()
	defer group.mu.Unlock()

	log.Printf("Heartbeat for consumer %s: group %s state: %s", req.ConsumerId, group.GroupId, group.State)
	if member, ok := group.Members[req.ConsumerId]; ok {
		member.LastHeartbeat = time.Now()
		if group.State == RebalanceInProgress {
			return &api.HeartbeatResponse{ErrorCode: api.ErrorCode_REBALANCE_IN_PROGRESS}, nil
		}
		return &api.HeartbeatResponse{ErrorCode: api.ErrorCode_OK}, nil
	}
	return &api.HeartbeatResponse{ErrorCode: api.ErrorCode_UNKNOWN_MEMBER_ID}, nil
}
