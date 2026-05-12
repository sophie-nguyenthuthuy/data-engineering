package remote

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/lsm-remote-compaction/internal/lsm"
	"github.com/lsm-remote-compaction/pkg/rpc"
)

// startWorker spins up a gRPC server and returns its address + cleanup func.
func startWorker(t *testing.T) string {
	t.Helper()
	w, err := NewWorker("test-worker", t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(grpc.ForceServerCodec(rpc.JSONCodec{}))
	rpc.RegisterCompactionServiceServer(srv, w)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func clientFor(t *testing.T, addr string) rpc.CompactionServiceClient {
	t.Helper()
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(rpc.JSONCodec{})),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })
	return rpc.NewCompactionServiceClient(cc)
}

func buildSST(t *testing.T, entries []lsm.Entry) []byte {
	t.Helper()
	path := t.TempDir() + "/test.sst"
	w, err := lsm.NewSSTableWriter(path, len(entries)+1)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := lsm.OpenSSTable(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	b, err := r.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestWorker_SubmitAndPoll(t *testing.T) {
	addr := startWorker(t)
	client := clientFor(t, addr)

	data1 := buildSST(t, []lsm.Entry{
		{Key: "aaa", Value: []byte("1")},
		{Key: "ccc", Value: []byte("3")},
	})
	data2 := buildSST(t, []lsm.Entry{
		{Key: "bbb", Value: []byte("2")},
		{Key: "ddd", Value: []byte("4")},
	})

	id := fmt.Sprintf("test-%d", time.Now().UnixNano())
	req := &rpc.CompactionRequest{
		CompactionID: id,
		InputFiles: []rpc.SSTFile{
			{Path: "f1.sst", Data: data1, Level: 0},
			{Path: "f2.sst", Data: data2, Level: 0},
		},
		TargetLevel: 1,
		QuorumSize:  1, // self-vote only
	}

	ctx := context.Background()
	resp, err := client.SubmitCompaction(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "accepted" {
		t.Fatalf("expected accepted, got %q: %s", resp.Status, resp.Error)
	}

	// poll for completion
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		st, err := client.GetStatus(ctx, &rpc.StatusRequest{CompactionID: id})
		if err != nil {
			t.Fatal(err)
		}
		switch st.Status {
		case "done":
			if st.OutputFile == nil || len(st.OutputFile.Data) == 0 {
				t.Fatal("done but no output data")
			}
			t.Logf("compaction done: %d output bytes", len(st.OutputFile.Data))
			return
		case "error":
			t.Fatalf("compaction error: %s", st.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("compaction did not complete in time")
}

func TestWorker_QuorumAck(t *testing.T) {
	addr := startWorker(t)
	client := clientFor(t, addr)
	ctx := context.Background()

	id := "quorum-test-1"
	// register a job with quorum=1 (self-vote only)
	_, _ = client.SubmitCompaction(ctx, &rpc.CompactionRequest{
		CompactionID: id,
		InputFiles:   []rpc.SSTFile{{Path: "x.sst", Data: buildSST(t, []lsm.Entry{{Key: "k", Value: []byte("v")}}), Level: 0}},
		TargetLevel:  1,
		QuorumSize:   1,
	})

	// vote from ourselves
	ackResp, err := client.AcknowledgeCompaction(ctx, &rpc.AckRequest{
		CompactionID: id,
		NodeID:       "ext-node-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ackResp.OK {
		t.Fatalf("ack rejected: %s", ackResp.Error)
	}
}
