// Package quorum implements dynamic Flexible-Paxos quorum management.
//
// The manager periodically samples read/write metrics and adjusts the Phase 1
// (Q1) and Phase 2 (Q2) quorum sizes to optimise for the observed workload,
// while always maintaining the Flexible Paxos safety invariant:
//
//	Q1 + Q2 > n   (where n = number of acceptors)
//
// Workload heuristics:
//
//	High R/W ratio  → many reads, few writes  → shrink Q2, grow Q1
//	                  (Phase 2 / write quorum is smaller → writes touch fewer nodes)
//	Low  R/W ratio  → many writes, few reads  → grow Q2, shrink Q1
//
// Note: in Paxos, Phase 2 is the "accept" phase (value commitment). A smaller
// Q2 means a write only needs acknowledgement from fewer acceptors, reducing
// write latency at the cost of requiring a larger Phase 1 quorum for the next
// leader election.
package quorum

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/paxos"
)

// Policy encodes adjustment thresholds.
type Policy struct {
	// When RWRatio > ReadHeavyThreshold, shrink Q2 (optimise writes).
	ReadHeavyThreshold float64
	// When RWRatio < WriteHeavyThreshold, shrink Q1 (optimise leader elections).
	WriteHeavyThreshold float64
	// SampleInterval controls how often the manager re-evaluates the workload.
	SampleInterval time.Duration
	// MinQ1, MinQ2 prevent pathologically small quorums.
	MinQ1 int
	MinQ2 int
}

// DefaultPolicy is suitable for a 5-node cluster.
var DefaultPolicy = Policy{
	ReadHeavyThreshold:  4.0,
	WriteHeavyThreshold: 0.25,
	SampleInterval:      5 * time.Second,
	MinQ1:               1,
	MinQ2:               1,
}

// ChangeListener is called whenever the manager installs a new quorum config.
type ChangeListener func(old, new paxos.QuorumConfig)

// Manager owns the current QuorumConfig and adjusts it over time.
type Manager struct {
	mu       sync.RWMutex
	current  paxos.QuorumConfig
	metrics  *Metrics
	policy   Policy
	epoch    uint64
	listeners []ChangeListener
}

// NewManager creates a manager starting from the given initial config.
func NewManager(initial paxos.QuorumConfig, m *Metrics, p Policy) (*Manager, error) {
	if !initial.Valid() {
		return nil, fmt.Errorf("initial config invalid")
	}
	return &Manager{
		current: initial,
		metrics: m,
		policy:  p,
		epoch:   initial.Epoch,
	}, nil
}

func (mgr *Manager) AddListener(l ChangeListener) {
	mgr.mu.Lock()
	mgr.listeners = append(mgr.listeners, l)
	mgr.mu.Unlock()
}

// Config returns the current quorum configuration.
func (mgr *Manager) Config() paxos.QuorumConfig {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	return mgr.current
}

// Run starts the background adjustment loop. It blocks until ctx is cancelled.
func (mgr *Manager) Run(ctx context.Context) {
	ticker := time.NewTicker(mgr.policy.SampleInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mgr.adjust()
		}
	}
}

// adjust computes the optimal Q1/Q2 for the observed R/W ratio and, if a
// change is warranted, installs a new config.
func (mgr *Manager) adjust() {
	snap := mgr.metrics.Snapshot()
	mgr.metrics.Reset()

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	n := len(mgr.current.Acceptors)
	q1, q2 := mgr.computeQuorums(n, snap.RWRatio)

	if q1 == mgr.current.Q1Size && q2 == mgr.current.Q2Size {
		return // no change
	}

	candidate := paxos.QuorumConfig{
		Acceptors: mgr.current.Acceptors,
		Q1Size:    q1,
		Q2Size:    q2,
		Epoch:     mgr.epoch + 1,
	}
	if !candidate.Valid() {
		return
	}

	old := mgr.current
	mgr.epoch++
	mgr.current = candidate

	for _, l := range mgr.listeners {
		go l(old, candidate)
	}
}

// computeQuorums derives (Q1, Q2) satisfying Q1+Q2 > n given the R/W ratio.
//
// Strategy:
//   - ratio >> 1 (read-heavy): optimise writes by shrinking Q2.
//     Use Q2 = max(MinQ2, n - floor(n * readFraction)).
//   - ratio << 1 (write-heavy): optimise leader election by shrinking Q1.
//   - balanced: classic majority.
func (mgr *Manager) computeQuorums(n int, ratio float64) (q1, q2 int) {
	p := mgr.policy

	maj := n/2 + 1

	switch {
	case ratio >= p.ReadHeavyThreshold:
		// Read-heavy: minimise Q2 while keeping Q1+Q2 > n.
		// Distribute toward smaller Q2: try to make Q2 proportional to
		// writes / (reads + writes).
		writeFrac := 1.0 / (1.0 + ratio)
		q2 = max(p.MinQ2, int(math.Ceil(float64(n)*writeFrac)))
		q1 = n - q2 + 1 // smallest Q1 that still satisfies the constraint
		q1 = max(q1, p.MinQ1)

	case ratio <= p.WriteHeavyThreshold:
		// Write-heavy: minimise Q1.
		readFrac := ratio / (1.0 + ratio)
		q1 = max(p.MinQ1, int(math.Ceil(float64(n)*readFrac)))
		q2 = n - q1 + 1
		q2 = max(q2, p.MinQ2)

	default:
		q1, q2 = maj, maj
	}

	// Clamp to [min, n].
	q1 = clamp(q1, p.MinQ1, n)
	q2 = clamp(q2, p.MinQ2, n)

	// Guarantee invariant: bump Q1 if needed.
	if q1+q2 <= n {
		q1 = n - q2 + 1
	}

	return q1, q2
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
