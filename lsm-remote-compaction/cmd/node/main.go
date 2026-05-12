// node is an LSM tree node with an HTTP key-value API.
// Compaction is delegated to a remote worker when --worker-addr is set;
// otherwise compaction runs locally.
//
// HTTP API:
//
//	PUT  /key/{key}        body = value
//	GET  /key/{key}
//	DELETE /key/{key}
//	GET  /stats
//
// Usage:
//
//	node --dir /tmp/lsm-data --http :8080 --worker-addr :9090
package main

import (
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/lsm-remote-compaction/internal/lsm"
	"github.com/lsm-remote-compaction/internal/remote"
)

func main() {
	dir         := flag.String("dir", os.TempDir()+"/lsm-data", "data directory")
	httpAddr    := flag.String("http", ":8080", "HTTP listen address")
	workerAddr  := flag.String("worker-addr", "", "remote compaction worker gRPC address (empty = local)")
	nodeID      := flag.String("id", "node-1", "node identifier for quorum")
	peers       := flag.String("peers", "", "comma-separated peer gRPC addresses for quorum")
	quorumSize  := flag.Int("quorum", 1, "minimum acks required for compaction commit")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := lsm.Config{
		Dir:                   *dir,
		MemTableSizeBytes:     4 * 1024 * 1024,
		L0CompactionThreshold: 4,
		Logger:                logger,
	}

	if *workerAddr != "" {
		var peerList []string
		if *peers != "" {
			peerList = strings.Split(*peers, ",")
		}
		cfg.Compactor = &remote.RemoteCompactor{
			WorkerAddr:    *workerAddr,
			NodeID:        *nodeID,
			PeerAddresses: peerList,
			QuorumSize:    int32(*quorumSize),
			Logger:        logger,
		}
		logger.Info("remote compaction enabled", "worker", *workerAddr, "quorum", *quorumSize)
	} else {
		logger.Info("local compaction enabled")
	}

	tree, err := lsm.Open(cfg)
	if err != nil {
		logger.Error("open lsm", "err", err)
		os.Exit(1)
	}
	defer tree.Close()

	mux := http.NewServeMux()

	mux.HandleFunc("PUT /key/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		val, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := tree.Put(key, val); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /key/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		val, found := tree.Get(key)
		if !found {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(val)
	})

	mux.HandleFunc("DELETE /key/{key}", func(w http.ResponseWriter, r *http.Request) {
		key := r.PathValue("key")
		if err := tree.Delete(key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte("LSM node running\n"))
	})

	srv := &http.Server{Addr: *httpAddr, Handler: mux}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutting down")
		srv.Close()
	}()

	logger.Info("LSM node listening", "http", *httpAddr, "dir", *dir)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		logger.Error("http", "err", err)
		os.Exit(1)
	}
}
