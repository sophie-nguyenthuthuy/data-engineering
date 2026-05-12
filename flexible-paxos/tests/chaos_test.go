package tests

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/linearizability"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/paxos"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/quorum"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/store"
)

// chaosCluster injects random partition/heal events during a workload.
type chaosCluster struct {
	transport *paxos.LocalTransport
	acceptors []string
	mu        sync.Mutex
	rng       *rand.Rand
	partitioned map[string]bool
}

func newChaosCluster(n int) (*chaosCluster, *paxos.LocalTransport) {
	_, transport := newCluster(n)
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("a%d", i+1)
	}
	return &chaosCluster{
		transport:   transport,
		acceptors:   ids,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		partitioned: make(map[string]bool),
	}, transport
}

// chaos randomly partitions and heals at most (maxPartitioned) nodes.
func (c *chaosCluster) chaos(ctx context.Context, maxPartitioned int) {
	for {
		select {
		case <-ctx.Done():
			// heal all on exit
			c.mu.Lock()
			for id := range c.partitioned {
				c.transport.Heal(id)
			}
			c.mu.Unlock()
			return
		case <-time.After(time.Duration(c.rng.Intn(30)+10) * time.Millisecond):
			c.mu.Lock()
			if len(c.partitioned) < maxPartitioned && c.rng.Float32() < 0.5 {
				// partition a random non-partitioned node
				for _, id := range c.acceptors {
					if !c.partitioned[id] {
						c.transport.Partition(id)
						c.partitioned[id] = true
						break
					}
				}
			} else {
				// heal a random partitioned node
				for id := range c.partitioned {
					c.transport.Heal(id)
					delete(c.partitioned, id)
					break
				}
			}
			c.mu.Unlock()
		}
	}
}

// TestChaosConsistency verifies that under minority network partitions the
// Paxos store satisfies two key properties:
//
//  1. Agreement: every completed read returns the same value for a given key
//     (at most one value is ever chosen per Paxos slot).
//
//  2. Linearisable reads: a strong Get() that starts after a Set() has
//     returned always sees the committed value (not a stale one).
func TestChaosConsistency(t *testing.T) {
	const (
		n        = 5
		duration = 2 * time.Second
	)

	cc, transport := newChaosCluster(n)
	cfg := paxos.Classic(cc.acceptors)
	m := quorum.NewMetrics()
	s := store.New(cfg, transport, m)

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Inject chaos: at most 2 of 5 nodes partitioned (minority).
	go cc.chaos(ctx, 2)

	// Phase 1: concurrent writes to "consensus-key".
	const writers = 4
	committed := make([]string, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func(i int) {
			defer wg.Done()
			val := fmt.Sprintf("writer%d", i)
			committed[i], errs[i] = s.Set(ctx, "consensus-key", val)
		}(i)
	}
	wg.Wait()

	// Collect all committed values from successful writes.
	var chosen string
	for i, err := range errs {
		if err != nil {
			t.Logf("writer %d error (expected under chaos): %v", i, err)
			continue
		}
		if chosen == "" {
			chosen = committed[i]
		} else if committed[i] != chosen {
			t.Fatalf("agreement violated: writer %d committed %q, others committed %q",
				i, committed[i], chosen)
		}
	}
	if chosen == "" {
		t.Skip("no write succeeded under chaos (acceptable with heavy partition)")
	}
	t.Logf("chosen value: %q", chosen)

	// Phase 2: concurrent reads must ALL see the chosen value.
	// We use fresh contexts (no timeout deadline) to ensure reads can complete.
	bgCtx := context.Background()
	const readers = 6
	results := make([]string, readers)
	readErrs := make([]error, readers)
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func(i int) {
			defer wg.Done()
			results[i], _, readErrs[i] = s.Get(bgCtx, "consensus-key")
		}(i)
	}
	wg.Wait()

	for i, err := range readErrs {
		if err != nil {
			t.Errorf("reader %d error: %v", i, err)
			continue
		}
		if results[i] != chosen {
			t.Errorf("linearizability violated: reader %d got %q, expected %q",
				i, results[i], chosen)
		}
	}
	t.Logf("all %d readers saw %q", readers, chosen)
}

// TestChaosLinearizability verifies the Elle-style verifier on a controlled
// sequential history that we know is linearisable.
func TestChaosLinearizability(t *testing.T) {
	// Build a sequential (non-overlapping) history on a single key with a
	// single proposer and verify it is classified as linearisable.
	_, transport := newCluster(5)
	cfg := paxos.Classic([]string{"a1", "a2", "a3", "a4", "a5"})
	m := quorum.NewMetrics()
	s := store.New(cfg, transport, m)
	rec := linearizability.NewRecorder()
	ctx := context.Background()

	key := "lin-key"

	// Write then read sequentially.
	wid := rec.Invoke(linearizability.OpWrite, key, "v1")
	committed, err := s.Set(ctx, key, "v1")
	rec.Complete(wid, committed, err)

	rid := rec.Invoke(linearizability.OpRead, key, "")
	got, _, err := s.Get(ctx, key)
	rec.Complete(rid, got, err)

	result := linearizability.Check(rec.History())
	if !result.OK {
		t.Fatalf("sequential history should be linearisable: %v", result)
	}
	t.Logf("chaos linearizability test passed: %d ops", len(rec.History()))
}

func TestFlexiblePaxosQuorumTransition(t *testing.T) {
	// Start read-optimised (Q1=4, Q2=2), prove proposals still work.
	// Then transition to write-optimised (Q1=2, Q2=4).
	_, transport := newCluster(5)
	ids := []string{"a1", "a2", "a3", "a4", "a5"}

	readOpt := paxos.QuorumConfig{Acceptors: ids, Q1Size: 4, Q2Size: 2}
	p, err := paxos.NewProposer("leader", readOpt, transport)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	// Phase 1: use read-optimised config.
	v1, err := p.Propose(ctx, paxos.Value("phase1-value"))
	if err != nil {
		t.Fatalf("read-opt propose: %v", err)
	}
	t.Logf("phase1 (read-opt Q1=4,Q2=2): chosen=%q", v1)

	// Transition to write-optimised.
	writeOpt := paxos.QuorumConfig{Acceptors: ids, Q1Size: 2, Q2Size: 4, Epoch: 1}
	if err := p.UpdateConfig(writeOpt); err != nil {
		t.Fatal(err)
	}

	v2, err := p.Propose(ctx, paxos.Value("phase2-value"))
	if err != nil {
		t.Fatalf("write-opt propose: %v", err)
	}
	t.Logf("phase2 (write-opt Q1=2,Q2=4): chosen=%q", v2)
}
