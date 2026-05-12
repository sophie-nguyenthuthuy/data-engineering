// Package remote implements the remote compaction protocol:
//
//  1. The local LSM node acts as leader and submits a CompactionRequest to a
//     remote worker over gRPC.
//  2. The worker compacts the files and stores the result.
//  3. The worker (acting as commit coordinator) contacts all peer nodes via
//     AcknowledgeCompaction, collecting votes.
//  4. Once a quorum of N/2+1 peers ack, the worker calls CommitCompaction on
//     itself and notifies the leader.  The leader then applies the new SSTable.
package remote

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lsm-remote-compaction/pkg/rpc"
)

// QuorumManager coordinates commit voting across peer nodes.
// Each compaction job tracks the set of acks it has received.
type QuorumManager struct {
	mu       sync.Mutex
	jobs     map[string]*quorumJob
	nodeID   string
	logger   *slog.Logger
}

type quorumJob struct {
	compactionID string
	quorumSize   int32
	acks         map[string]struct{}
	committed    atomic.Bool
	doneCh       chan struct{}
}

// NewQuorumManager creates a manager for nodeID.
func NewQuorumManager(nodeID string, logger *slog.Logger) *QuorumManager {
	return &QuorumManager{
		jobs:   make(map[string]*quorumJob),
		nodeID: nodeID,
		logger: logger,
	}
}

// Register creates a new quorum tracking entry for compactionID.
// quorumSize is the minimum number of acks (including self) required.
func (q *QuorumManager) Register(compactionID string, quorumSize int32) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.jobs[compactionID] = &quorumJob{
		compactionID: compactionID,
		quorumSize:   quorumSize,
		acks:         make(map[string]struct{}),
		doneCh:       make(chan struct{}),
	}
}

// RecordAck records a vote from nodeID for compactionID.
// Returns (reached quorum, error).
func (q *QuorumManager) RecordAck(compactionID, nodeID string) (bool, error) {
	q.mu.Lock()
	job, ok := q.jobs[compactionID]
	if !ok {
		q.mu.Unlock()
		return false, fmt.Errorf("unknown compaction %s", compactionID)
	}
	job.acks[nodeID] = struct{}{}
	reached := int32(len(job.acks)) >= job.quorumSize
	if reached && !job.committed.Load() {
		job.committed.Store(true)
		close(job.doneCh)
	}
	q.mu.Unlock()
	return reached, nil
}

// WaitCommit blocks until quorum is reached or ctx is cancelled.
func (q *QuorumManager) WaitCommit(ctx context.Context, compactionID string) error {
	q.mu.Lock()
	job, ok := q.jobs[compactionID]
	q.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown compaction %s", compactionID)
	}
	select {
	case <-job.doneCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cleanup removes a completed job's tracking state.
func (q *QuorumManager) Cleanup(compactionID string) {
	q.mu.Lock()
	delete(q.jobs, compactionID)
	q.mu.Unlock()
}

// SolicitAcks contacts peers concurrently and records their votes.
// selfVote counts the worker itself as one ack.
// Returns number of successful acks (including self).
func (q *QuorumManager) SolicitAcks(
	ctx context.Context,
	compactionID string,
	peers []string,
) (int, error) {
	// cast self-vote first
	reached, err := q.RecordAck(compactionID, q.nodeID)
	if err != nil {
		return 0, err
	}
	acks := 1
	if reached {
		return acks, nil
	}

	type result struct {
		ok  bool
		err error
	}
	resCh := make(chan result, len(peers))

	for _, addr := range peers {
		go func(addr string) {
			ok, err := solictiOne(ctx, compactionID, q.nodeID, addr, q.logger)
			resCh <- result{ok, err}
		}(addr)
	}

	deadline := time.After(30 * time.Second)
	for i := 0; i < len(peers); i++ {
		select {
		case r := <-resCh:
			if r.ok {
				acks++
				reached, _ = q.RecordAck(compactionID, fmt.Sprintf("peer-%d", i))
				if reached {
					return acks, nil
				}
			} else {
				q.logger.Warn("peer ack failed", "err", r.err)
			}
		case <-deadline:
			return acks, fmt.Errorf("ack solicitation timed out with %d/%d acks", acks, len(peers)+1)
		case <-ctx.Done():
			return acks, ctx.Err()
		}
	}
	return acks, nil
}

func solictiOne(ctx context.Context, compactionID, callerID, addr string, log *slog.Logger) (bool, error) {
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.JSONCodec{})),
	)
	if err != nil {
		return false, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer cc.Close()

	client := rpc.NewCompactionServiceClient(cc)
	resp, err := client.AcknowledgeCompaction(ctx, &rpc.AckRequest{
		CompactionID: compactionID,
		NodeID:       callerID,
	})
	if err != nil {
		return false, err
	}
	if !resp.OK {
		return false, fmt.Errorf("peer rejected ack: %s", resp.Error)
	}
	return true, nil
}
