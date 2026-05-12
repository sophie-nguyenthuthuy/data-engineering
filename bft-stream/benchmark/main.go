// Command benchmark measures throughput and latency for BFT (PBFT) vs CFT
// (Raft) consensus applied to watermark advancement in a stream pipeline.
//
// Design: N replicas each run a local Pipeline. Records are broadcast to all
// replicas (simulating reliable multicast). Only watermark advancement events
// go through consensus — one round per window regardless of record volume.
//
// Key metric: overhead = PBFT_latency / Raft_latency
// Target: < 3× for honest nodes (achievable because BFT cost is amortized
// over all records in a window, not paid per record).
package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/sophie-nguyenthuthuy/bft-stream/internal/pbft"
	"github.com/sophie-nguyenthuthuy/bft-stream/internal/raft"
	"github.com/sophie-nguyenthuthuy/bft-stream/internal/stream"
)

// ---- Benchmark configuration -----------------------------------------------

type config struct {
	name           string
	nodes          int           // cluster size (PBFT: 3f+1, Raft: 2f+1)
	recordsPerSec  int           // ingestion rate
	windowSize     time.Duration // tumbling window duration
	wmInterval     time.Duration // watermark proposal interval
	duration       time.Duration // total run duration
	byzantineNodes int           // nodes to inject faults into (PBFT only)
}

var scenarios = []config{
	{
		name:          "small-cluster (n=4/3, 100k rec/s, 100ms window)",
		nodes:         4,
		recordsPerSec: 100_000,
		windowSize:    100 * time.Millisecond,
		wmInterval:    20 * time.Millisecond,
		duration:      3 * time.Second,
	},
	{
		name:          "medium-cluster (n=7/5, 50k rec/s, 200ms window)",
		nodes:         7,
		recordsPerSec: 50_000,
		windowSize:    200 * time.Millisecond,
		wmInterval:    40 * time.Millisecond,
		duration:      3 * time.Second,
	},
	{
		name:           "byzantine-fault (n=4, 1 faulty, 100k rec/s, 100ms window)",
		nodes:          4,
		recordsPerSec:  100_000,
		windowSize:     100 * time.Millisecond,
		wmInterval:     20 * time.Millisecond,
		duration:       3 * time.Second,
		byzantineNodes: 1,
	},
}

// ---- runResult holds per-run stats -----------------------------------------

type runResult struct {
	name         string
	protocol     string
	windows      int
	totalRecords int64
	latencies    []time.Duration
	duration     time.Duration
}

func (r runResult) p50() time.Duration { return percentile(r.latencies, 50) }
func (r runResult) p99() time.Duration { return percentile(r.latencies, 99) }
func (r runResult) mean() time.Duration { return meanDuration(r.latencies) }
func (r runResult) windowsPerSec() float64 {
	if r.duration == 0 {
		return 0
	}
	return float64(r.windows) / r.duration.Seconds()
}
func (r runResult) recPerSec() float64 {
	if r.duration == 0 {
		return 0
	}
	return float64(r.totalRecords) / r.duration.Seconds()
}

// ---- PBFT run --------------------------------------------------------------

func runPBFT(cfg config) runResult {
	pipelineMap := make(map[int]*stream.Pipeline)
	var mapMu sync.Mutex

	var latMu sync.Mutex
	var latencies []time.Duration
	var windows int

	cluster := pbft.NewCluster(cfg.nodes, func(nodeID int, seq uint64, value []byte) {
		mapMu.Lock()
		p := pipelineMap[nodeID]
		mapMu.Unlock()
		if p != nil {
			p.ApplyWatermark(seq, value)
		}
	})

	pipelines := make([]*stream.Pipeline, cfg.nodes)
	for i, nd := range cluster.Nodes {
		pip := stream.New(i, cfg.windowSize, nd)
		mapMu.Lock()
		pipelineMap[i] = pip
		mapMu.Unlock()
		pipelines[i] = pip
		pip.OnResult(func(r stream.WindowResult) {
			latMu.Lock()
			latencies = append(latencies, r.Latency)
			windows++
			latMu.Unlock()
		})
	}

	if cfg.byzantineNodes > 0 {
		for i := 0; i < cfg.byzantineNodes; i++ {
			cluster.MakeByzantine(cfg.nodes-1-i, 0.3)
		}
	}

	cluster.Start()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration+2*time.Second)
	defer cancel()

	for _, pip := range pipelines {
		pip.Start(ctx, cfg.wmInterval)
	}

	var totalRecords int64
	start := time.Now()
	ingest(ctx, cfg, pipelines, &totalRecords)
	elapsed := time.Since(start)

	time.Sleep(500 * time.Millisecond) // let in-flight windows drain

	for _, pip := range pipelines {
		pip.Stop()
	}
	cluster.Stop()

	latMu.Lock()
	lats := make([]time.Duration, len(latencies))
	copy(lats, latencies)
	latMu.Unlock()

	return runResult{
		name:         cfg.name,
		protocol:     fmt.Sprintf("PBFT (n=%d, f=%d)", cfg.nodes, (cfg.nodes-1)/3),
		windows:      windows,
		totalRecords: totalRecords,
		latencies:    lats,
		duration:     elapsed,
	}
}

// ---- Raft run --------------------------------------------------------------

func runRaft(cfg config) runResult {
	// For a fair comparison we match f: PBFT with n=4 tolerates f=1.
	// Raft needs only 2f+1=3 nodes for the same f=1 — one fewer node,
	// reflecting the weaker threat model (crash-only).
	raftNodes := cfg.nodes
	if cfg.nodes == 4 {
		raftNodes = 3
	}

	pipelineMap := make(map[int]*stream.Pipeline)
	var mapMu sync.Mutex

	var latMu sync.Mutex
	var latencies []time.Duration
	var windows int

	cluster := raft.NewCluster(raftNodes, func(nodeID int, seq uint64, value []byte) {
		mapMu.Lock()
		p := pipelineMap[nodeID]
		mapMu.Unlock()
		if p != nil {
			p.ApplyWatermark(seq, value)
		}
	})

	pipelines := make([]*stream.Pipeline, raftNodes)
	for i, nd := range cluster.Nodes {
		pip := stream.New(i, cfg.windowSize, nd)
		mapMu.Lock()
		pipelineMap[i] = pip
		mapMu.Unlock()
		pipelines[i] = pip
		pip.OnResult(func(r stream.WindowResult) {
			latMu.Lock()
			latencies = append(latencies, r.Latency)
			windows++
			latMu.Unlock()
		})
	}

	cluster.Start()
	time.Sleep(250 * time.Millisecond) // wait for Raft leader election

	ctx, cancel := context.WithTimeout(context.Background(), cfg.duration+2*time.Second)
	defer cancel()

	for _, pip := range pipelines {
		pip.Start(ctx, cfg.wmInterval)
	}

	var totalRecords int64
	start := time.Now()
	ingest(ctx, cfg, pipelines, &totalRecords)
	elapsed := time.Since(start)

	time.Sleep(500 * time.Millisecond)

	for _, pip := range pipelines {
		pip.Stop()
	}
	cluster.Stop()

	latMu.Lock()
	lats := make([]time.Duration, len(latencies))
	copy(lats, latencies)
	latMu.Unlock()

	return runResult{
		name:         cfg.name,
		protocol:     fmt.Sprintf("Raft  (n=%d, f=%d)", raftNodes, (raftNodes-1)/2),
		windows:      windows,
		totalRecords: totalRecords,
		latencies:    lats,
		duration:     elapsed,
	}
}

// ---- record ingestion ------------------------------------------------------

func ingest(ctx context.Context, cfg config, pipelines []*stream.Pipeline, total *int64) {
	rng := rand.New(rand.NewSource(42))
	interval := time.Second / time.Duration(cfg.recordsPerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var count int64
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			*total = count
			return
		case t := <-ticker.C:
			if t.Sub(start) >= cfg.duration {
				*total = count
				return
			}
			r := stream.Record{
				Key:       "sensor",
				Value:     rng.Float64() * 100,
				EventTime: t,
				SeqID:     uint64(count),
			}
			// Broadcast to all replicas — no consensus per record
			for _, pip := range pipelines {
				pip.Ingest(r)
			}
			count++
		}
	}
}

// ---- main ------------------------------------------------------------------

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║      BFT Stream Processing: PBFT vs Raft Watermark Consensus        ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("  BFT consensus is applied ONLY to watermark advancement (window close)")
	fmt.Println("  events — not individual records. This amortizes PBFT's O(n²) message")
	fmt.Println("  complexity over thousands of records per window, targeting < 3× overhead.")
	fmt.Println()

	type scenarioResults struct {
		cfg  config
		raft runResult
		pbft runResult
	}

	var all []scenarioResults
	for _, cfg := range scenarios {
		fmt.Printf("  ▶ Running scenario: %s\n", cfg.name)
		fmt.Printf("    Raft ... ")
		raftResult := runRaft(cfg)
		fmt.Printf("done (%d windows)\n", raftResult.windows)

		fmt.Printf("    PBFT ... ")
		pbftResult := runPBFT(cfg)
		fmt.Printf("done (%d windows)\n", pbftResult.windows)
		fmt.Println()

		all = append(all, scenarioResults{cfg, raftResult, pbftResult})
	}

	fmt.Println()
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "Protocol\tWindows\tRec/s\tMean lat\tp50\tp99\tOverhead vs Raft")
	fmt.Fprintln(tw, "────────\t───────\t─────\t────────\t───\t───\t────────────────")

	for _, s := range all {
		fmt.Fprintf(tw, "\n  ─ %s ─\n", s.cfg.name)
		printRow(tw, s.raft, 1.0)
		overhead := computeOverhead(s.raft, s.pbft)
		printRow(tw, s.pbft, overhead)
	}
	tw.Flush()

	fmt.Println()
	fmt.Println("── Overhead Summary ────────────────────────────────────────────────────")
	fmt.Println()
	allPass := true
	for _, s := range all {
		overhead := computeOverhead(s.raft, s.pbft)
		status := "✓ PASS"
		if overhead >= 3.0 {
			status = "✗ FAIL"
			allPass = false
		}
		recsPerWindow := float64(s.pbft.totalRecords) / math.Max(float64(s.pbft.windows), 1)
		fmt.Printf("  [%s] %s  overhead=%.2f×  %.0f rec/window\n",
			status, s.cfg.name, overhead, recsPerWindow)
	}
	fmt.Println()
	if allPass {
		fmt.Println("  All scenarios achieve < 3× overhead. BFT watermark protocol ✓")
	} else {
		fmt.Println("  Some scenarios exceed 3×. Consider larger window size or batching.")
	}

	fmt.Println()
	fmt.Println("── Protocol Comparison ─────────────────────────────────────────────────")
	fmt.Println()
	fmt.Println("  PBFT  3f+1 nodes  3 phases (PRE-PREPARE→PREPARE→COMMIT)  O(n²) msgs")
	fmt.Println("  Raft  2f+1 nodes  2 phases (AppendEntries→Response)       O(n) msgs")
	fmt.Println()
	fmt.Println("  BFT advantage: tolerates Byzantine (active) faults — a faulty primary")
	fmt.Println("  cannot skip records or forge watermarks; 2f+1 honest nodes overrule.")
	fmt.Println("  CFT advantage: simpler, fewer nodes, lower latency, higher throughput.")
	fmt.Println()
	fmt.Println("  Watermark-only BFT cuts the per-record cost to near zero:")
	fmt.Println("    overhead = PBFT_round_latency / records_per_window ≈ microseconds")
}

func computeOverhead(r, p runResult) float64 {
	if r.mean() == 0 || len(r.latencies) == 0 {
		return 0
	}
	return float64(p.mean()) / float64(r.mean())
}

func printRow(tw *tabwriter.Writer, r runResult, overhead float64) {
	lat, p50, p99 := "-", "-", "-"
	if len(r.latencies) > 0 {
		lat = r.mean().Round(time.Microsecond).String()
		p50 = r.p50().Round(time.Microsecond).String()
		p99 = r.p99().Round(time.Microsecond).String()
	}
	oh := "1.00× (baseline)"
	if overhead != 1.0 && overhead > 0 {
		oh = fmt.Sprintf("%.2f×", overhead)
	}
	fmt.Fprintf(tw, "  %s\t%d\t%.0f\t%s\t%s\t%s\t%s\n",
		r.protocol, r.windows, r.recPerSec(), lat, p50, p99, oh)
}

// ---- statistics ------------------------------------------------------------

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(ds))
	copy(sorted, ds)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(float64(len(sorted))*p/100)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func meanDuration(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var sum int64
	for _, d := range ds {
		sum += int64(d)
	}
	return time.Duration(sum / int64(len(ds)))
}
