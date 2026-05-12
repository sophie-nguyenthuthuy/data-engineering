// worker is the remote compaction node.
// It runs a gRPC server that accepts SSTable data, compacts it, drives
// quorum commit, and returns the merged SSTable to the originating LSM node.
//
// Usage:
//
//	worker --addr :9090 --id worker-1 --workdir /tmp/worker
package main

import (
	"flag"
	"log/slog"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/lsm-remote-compaction/internal/remote"
	"github.com/lsm-remote-compaction/pkg/rpc"
)

func main() {
	addr    := flag.String("addr", ":9090", "gRPC listen address")
	nodeID  := flag.String("id", "worker-1", "unique node identifier")
	workDir := flag.String("workdir", os.TempDir()+"/lsm-worker", "scratch directory for temp files")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	worker, err := remote.NewWorker(*nodeID, *workDir, logger)
	if err != nil {
		logger.Error("init worker", "err", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("listen", "err", err)
		os.Exit(1)
	}

	srv := grpc.NewServer(
		grpc.ForceServerCodec(rpc.JSONCodec{}),
	)
	rpc.RegisterCompactionServiceServer(srv, worker)

	logger.Info("compaction worker listening", "addr", *addr, "node_id", *nodeID)
	if err := srv.Serve(lis); err != nil {
		logger.Error("serve", "err", err)
		os.Exit(1)
	}
}
