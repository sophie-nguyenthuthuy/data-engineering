package remote

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lsm-remote-compaction/internal/lsm"
	"github.com/lsm-remote-compaction/pkg/rpc"
)

// RemoteCompactor implements lsm.CompactionHandler.  It ships SSTable data to
// a remote Worker over gRPC and polls until the job is complete.
type RemoteCompactor struct {
	// WorkerAddr is the gRPC address of the remote worker (host:port).
	WorkerAddr string
	// NodeID identifies this LSM node in the quorum protocol.
	NodeID string
	// PeerAddresses are other LSM nodes the worker should contact for quorum.
	PeerAddresses []string
	// QuorumSize is the minimum number of acks (including worker self-vote).
	QuorumSize int32
	Logger     *slog.Logger
}

// Compact ships the input SSTables to the remote worker, polls for completion,
// writes the result to outPath, and returns its metadata.
//
// The local LSM node continues serving reads/writes while this call blocks in
// the background compactLoop goroutine.
func (rc *RemoteCompactor) Compact(
	ctx context.Context,
	inputs []*lsm.SSTableReader,
	outPath string,
	targetLevel int,
	_ bool,
) (*lsm.SSTableMeta, error) {
	cc, err := grpc.NewClient(rc.WorkerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.JSONCodec{})),
	)
	if err != nil {
		return nil, fmt.Errorf("dial worker %s: %w", rc.WorkerAddr, err)
	}
	defer cc.Close()
	client := rpc.NewCompactionServiceClient(cc)

	// serialise SSTable files into the request
	compactionID := fmt.Sprintf("%s-%d-%d", rc.NodeID, targetLevel, time.Now().UnixNano())
	req, err := rc.buildRequest(compactionID, inputs, targetLevel)
	if err != nil {
		return nil, err
	}

	submitResp, err := client.SubmitCompaction(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("submit compaction: %w", err)
	}
	if submitResp.Status != "accepted" {
		return nil, fmt.Errorf("worker rejected compaction: %s", submitResp.Error)
	}
	rc.Logger.Info("compaction submitted to worker",
		"id", compactionID, "worker", rc.WorkerAddr, "files", len(inputs))

	// poll until done
	output, err := rc.poll(ctx, client, compactionID)
	if err != nil {
		return nil, err
	}

	// write the result to the local SSTable path
	if err := os.WriteFile(outPath, output.Data, 0o644); err != nil {
		return nil, fmt.Errorf("write remote output: %w", err)
	}
	// request final commit from worker (acknowledges quorum on our side too)
	commitResp, err := client.CommitCompaction(ctx, &rpc.CommitRequest{CompactionID: compactionID})
	if err != nil {
		rc.Logger.Warn("commit call failed (data already written)", "err", err)
	} else if !commitResp.Committed {
		rc.Logger.Warn("commit not confirmed", "err", commitResp.Error)
	}

	fi, _ := os.Stat(outPath)
	var size int64
	if fi != nil {
		size = fi.Size()
	}
	return &lsm.SSTableMeta{
		Path:  outPath,
		Level: targetLevel,
		Size:  size,
	}, nil
}

func (rc *RemoteCompactor) buildRequest(
	id string,
	inputs []*lsm.SSTableReader,
	targetLevel int,
) (*rpc.CompactionRequest, error) {
	files := make([]rpc.SSTFile, len(inputs))
	for i, r := range inputs {
		data, err := r.Bytes()
		if err != nil {
			return nil, fmt.Errorf("read sst %s: %w", r.Meta().Path, err)
		}
		files[i] = rpc.SSTFile{
			Path:  r.Meta().Path,
			Data:  data,
			Level: int32(r.Meta().Level),
		}
	}
	qs := rc.QuorumSize
	if qs < 1 {
		qs = 1
	}
	return &rpc.CompactionRequest{
		CompactionID:  id,
		InputFiles:    files,
		TargetLevel:   int32(targetLevel),
		PeerAddresses: rc.PeerAddresses,
		QuorumSize:    qs,
	}, nil
}

func (rc *RemoteCompactor) poll(ctx context.Context, client rpc.CompactionServiceClient, id string) (*rpc.SSTFile, error) {
	backoff := 200 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		resp, err := client.GetStatus(ctx, &rpc.StatusRequest{CompactionID: id})
		if err != nil {
			return nil, fmt.Errorf("poll status: %w", err)
		}
		switch resp.Status {
		case "done":
			if resp.OutputFile == nil {
				return nil, fmt.Errorf("worker returned done but no output")
			}
			return resp.OutputFile, nil
		case "error":
			return nil, fmt.Errorf("remote compaction error: %s", resp.Error)
		case "running":
			rc.Logger.Debug("compaction running", "id", id)
			if backoff < 5*time.Second {
				backoff = min(backoff*2, 5*time.Second)
			}
		default:
			return nil, fmt.Errorf("unexpected status %q", resp.Status)
		}
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
