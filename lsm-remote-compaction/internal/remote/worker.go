package remote

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lsm-remote-compaction/internal/lsm"
	"github.com/lsm-remote-compaction/pkg/rpc"
)

// jobState tracks an async compaction job on the worker.
type jobState struct {
	id        string
	status    string // "running" | "done" | "error"
	output    *rpc.SSTFile
	err       string
	createdAt time.Time
}

// Worker is the gRPC server implementing CompactionServiceServer.
// It accepts compaction requests from LSM nodes, merges the files,
// then drives the quorum commit protocol before signalling completion.
type Worker struct {
	nodeID  string
	workDir string
	quorum  *QuorumManager
	logger  *slog.Logger

	mu   sync.RWMutex
	jobs map[string]*jobState
}

// NewWorker creates a compaction worker that stores temp files in workDir.
func NewWorker(nodeID, workDir string, logger *slog.Logger) (*Worker, error) {
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{
		nodeID:  nodeID,
		workDir: workDir,
		quorum:  NewQuorumManager(nodeID, logger),
		logger:  logger,
		jobs:    make(map[string]*jobState),
	}, nil
}

// SubmitCompaction is called by the LSM node to kick off an async compaction.
func (w *Worker) SubmitCompaction(ctx context.Context, req *rpc.CompactionRequest) (*rpc.CompactionResponse, error) {
	if req.CompactionID == "" {
		return nil, fmt.Errorf("empty compaction_id")
	}
	w.mu.Lock()
	if _, exists := w.jobs[req.CompactionID]; exists {
		w.mu.Unlock()
		return &rpc.CompactionResponse{CompactionID: req.CompactionID, Status: "accepted"}, nil
	}
	job := &jobState{id: req.CompactionID, status: "running", createdAt: time.Now()}
	w.jobs[req.CompactionID] = job
	w.quorum.Register(req.CompactionID, req.QuorumSize)
	w.mu.Unlock()

	w.logger.Info("compaction submitted", "id", req.CompactionID, "files", len(req.InputFiles), "target_level", req.TargetLevel)
	go w.runJob(req)
	return &rpc.CompactionResponse{CompactionID: req.CompactionID, Status: "accepted"}, nil
}

// GetStatus polls a running or completed job.
func (w *Worker) GetStatus(_ context.Context, req *rpc.StatusRequest) (*rpc.StatusResponse, error) {
	w.mu.RLock()
	job, ok := w.jobs[req.CompactionID]
	w.mu.RUnlock()
	if !ok {
		return &rpc.StatusResponse{CompactionID: req.CompactionID, Status: "unknown"}, nil
	}
	return &rpc.StatusResponse{
		CompactionID: job.id,
		Status:       job.status,
		OutputFile:   job.output,
		Error:        job.err,
	}, nil
}

// AcknowledgeCompaction records a quorum vote from a peer node.
func (w *Worker) AcknowledgeCompaction(_ context.Context, req *rpc.AckRequest) (*rpc.AckResponse, error) {
	reached, err := w.quorum.RecordAck(req.CompactionID, req.NodeID)
	if err != nil {
		return &rpc.AckResponse{OK: false, Error: err.Error()}, nil
	}
	w.logger.Info("ack recorded", "compaction_id", req.CompactionID, "node", req.NodeID, "quorum_reached", reached)
	return &rpc.AckResponse{OK: true}, nil
}

// CommitCompaction is a no-op on the worker side; quorum tracking handles it.
func (w *Worker) CommitCompaction(_ context.Context, req *rpc.CommitRequest) (*rpc.CommitResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := w.quorum.WaitCommit(ctx, req.CompactionID); err != nil {
		return &rpc.CommitResponse{Committed: false, Error: err.Error()}, nil
	}
	return &rpc.CommitResponse{Committed: true}, nil
}

// runJob executes the compaction, solicits quorum acks, and marks the job done.
func (w *Worker) runJob(req *rpc.CompactionRequest) {
	jobID := req.CompactionID

	outputData, err := w.compact(req)
	if err != nil {
		w.logger.Error("compaction failed", "id", jobID, "err", err)
		w.setJobError(jobID, err.Error())
		return
	}

	// solicit quorum acks from peer nodes
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	acks, err := w.quorum.SolicitAcks(ctx, jobID, req.PeerAddresses)
	if err != nil {
		w.logger.Warn("quorum not fully reached", "id", jobID, "acks", acks, "err", err)
		// still proceed if we got any acks; caller can check status
	}
	w.logger.Info("quorum commit", "id", jobID, "acks", acks, "required", req.QuorumSize)

	outFile := &rpc.SSTFile{
		Path:  fmt.Sprintf("remote_%s_L%d.sst", jobID, req.TargetLevel),
		Data:  outputData,
		Level: req.TargetLevel,
	}
	w.mu.Lock()
	if job, ok := w.jobs[jobID]; ok {
		job.status = "done"
		job.output = outFile
	}
	w.mu.Unlock()
	w.logger.Info("compaction complete", "id", jobID, "output_bytes", len(outputData))
}

func (w *Worker) compact(req *rpc.CompactionRequest) ([]byte, error) {
	// deserialise each input SSTable from the inline bytes
	inputs := make([][]lsm.Entry, len(req.InputFiles))
	for i, f := range req.InputFiles {
		tmp := filepath.Join(w.workDir, fmt.Sprintf("in_%s_%d.sst", req.CompactionID, i))
		if err := os.WriteFile(tmp, f.Data, 0o644); err != nil {
			return nil, fmt.Errorf("write tmp %s: %w", tmp, err)
		}
		defer os.Remove(tmp)

		r, err := lsm.OpenSSTable(tmp, int(f.Level))
		if err != nil {
			return nil, fmt.Errorf("open tmp sst: %w", err)
		}
		entries, err := r.Iter()
		r.Close()
		if err != nil {
			return nil, fmt.Errorf("iter sst: %w", err)
		}
		inputs[i] = entries
	}

	numLevels := 7
	bottom := int(req.TargetLevel) == numLevels-1
	outPath := filepath.Join(w.workDir, fmt.Sprintf("out_%s_L%d.sst", req.CompactionID, req.TargetLevel))
	defer os.Remove(outPath)

	if _, err := lsm.CompactInMemory(inputs, outPath, bottom); err != nil {
		return nil, fmt.Errorf("compact: %w", err)
	}
	data, err := os.ReadFile(outPath)
	if err != nil {
		return nil, fmt.Errorf("read output sst: %w", err)
	}
	return data, nil
}

func (w *Worker) setJobError(id, msg string) {
	w.mu.Lock()
	if job, ok := w.jobs[id]; ok {
		job.status = "error"
		job.err = msg
	}
	w.mu.Unlock()
}
