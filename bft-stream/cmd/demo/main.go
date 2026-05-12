// Command demo runs a live PBFT stream pipeline with real-time output,
// showing watermark advancement and window commits as they happen.
package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/sophie-nguyenthuthuy/bft-stream/internal/pbft"
	"github.com/sophie-nguyenthuthuy/bft-stream/internal/stream"
	"github.com/sophie-nguyenthuthuy/bft-stream/internal/transport"
)

func main() {
	const (
		numNodes      = 4               // 3f+1, f=1
		windowSize    = 500 * time.Millisecond
		wmInterval    = 100 * time.Millisecond
		recordsPerSec = 1000
		runDuration   = 10 * time.Second
	)

	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║   BFT Stream Demo — PBFT Watermark Consensus (f=1)      ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Printf("  Cluster   : %d nodes (tolerates 1 Byzantine fault)\n", numNodes)
	fmt.Printf("  Window    : %s tumbling\n", windowSize)
	fmt.Printf("  Records   : %d/sec\n", recordsPerSec)
	fmt.Printf("  Duration  : %s\n", runDuration)
	fmt.Println()
	fmt.Println("  BFT consensus fires ONCE per window (not per record).")
	fmt.Println("  Honest replicas validate watermark against local state.")
	fmt.Println()

	pipelineMap := make(map[int]*stream.Pipeline)

	cluster := pbft.NewCluster(numNodes, func(nodeID int, seq uint64, value []byte) {
		if p, ok := pipelineMap[nodeID]; ok {
			p.ApplyWatermark(seq, value)
		}
	})

	pipelines := make([]*stream.Pipeline, numNodes)
	for i, nd := range cluster.Nodes {
		pip := stream.New(i, windowSize, nd)
		pipelineMap[i] = pip
		pipelines[i] = pip
	}

	// Only print results from node 0 (primary) to avoid duplicate output
	primary := pipelines[0]
	primary.OnResult(func(r stream.WindowResult) {
		fmt.Printf("  [%s] window=%s  count=%d  sum=%.1f  mean=%.2f  latency=%s\n",
			r.CommittedAt.Format("15:04:05.000"),
			r.WindowID,
			r.Count,
			r.Sum,
			r.Mean,
			r.Latency.Round(time.Microsecond),
		)
	})

	cluster.Start()
	ctx, cancel := context.WithTimeout(context.Background(), runDuration+2*time.Second)
	defer cancel()

	for _, pip := range pipelines {
		pip.Start(ctx, wmInterval)
	}

	// Inject a Byzantine node mid-run
	go func() {
		time.Sleep(5 * time.Second)
		fmt.Println("\n  ⚡ Injecting Byzantine fault on node 3 (30% message drop + corruption)")
		cluster.MakeByzantine(3, 0.3)
		time.Sleep(3 * time.Second)
		fmt.Println("  ✓ Byzantine node healed — resuming normal operation")
		cluster.Bus.SetConfig(3, transport.Config{})
	}()

	rng := rand.New(rand.NewSource(99))
	ticker := time.NewTicker(time.Second / time.Duration(recordsPerSec))
	defer ticker.Stop()

	start := time.Now()
	var count int64
	for {
		select {
		case <-ctx.Done():
			goto done
		case t := <-ticker.C:
			if t.Sub(start) >= runDuration {
				goto done
			}
			r := stream.Record{
				Key:       "temp_sensor",
				Value:     20 + rng.Float64()*10,
				EventTime: t,
				SeqID:     uint64(count),
			}
			for _, pip := range pipelines {
				pip.Ingest(r)
			}
			count++
		}
	}

done:
	time.Sleep(500 * time.Millisecond)

	for _, pip := range pipelines {
		pip.Stop()
	}
	cluster.Stop()

	fmt.Println()
	fmt.Println("── Final Stats ──────────────────────────────────────────")
	for _, pip := range pipelines {
		s := pip.Stats()
		fmt.Printf("  Node %d  records=%d  windows=%d  consensusOps=%d  lateDropped=%d\n",
			s.NodeID, s.RecordsIn, s.WindowsOut, s.ConsensusOps, s.LateDropped)
	}
}
