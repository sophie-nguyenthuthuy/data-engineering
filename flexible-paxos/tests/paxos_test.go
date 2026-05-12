package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/paxos"
)

// --- helpers ------------------------------------------------------------------

func newCluster(n int) ([]*paxos.Acceptor, *paxos.LocalTransport) {
	transport := paxos.NewLocalTransport()
	acceptors := make([]*paxos.Acceptor, n)
	ids := make([]string, n)
	for i := range acceptors {
		id := fmt.Sprintf("a%d", i+1)
		acceptors[i] = paxos.NewAcceptor(id)
		transport.Register(acceptors[i])
		ids[i] = id
	}
	return acceptors, transport
}

func classicConfig(n int) paxos.QuorumConfig {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("a%d", i+1)
	}
	return paxos.Classic(ids)
}

func flexConfig(acceptors []string, q1, q2 int) paxos.QuorumConfig {
	return paxos.QuorumConfig{Acceptors: acceptors, Q1Size: q1, Q2Size: q2}
}

// --- Classic Paxos correctness -----------------------------------------------

func TestClassicPaxosSingleProposer(t *testing.T) {
	_, transport := newCluster(5)
	cfg := classicConfig(5)

	p, err := paxos.NewProposer("leader", cfg, transport)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	got, err := p.Propose(ctx, paxos.Value("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("expected 'hello', got %q", got)
	}
}

func TestClassicPaxosTwoProposersOneValue(t *testing.T) {
	_, transport := newCluster(5)
	cfg := classicConfig(5)

	p1, _ := paxos.NewProposer("l1", cfg, transport)
	p2, _ := paxos.NewProposer("l2", cfg, transport)

	ctx := context.Background()
	var wg sync.WaitGroup
	results := make([]paxos.Value, 2)
	errs := make([]error, 2)

	wg.Add(2)
	go func() { defer wg.Done(); results[0], errs[0] = p1.Propose(ctx, paxos.Value("v1")) }()
	go func() { defer wg.Done(); results[1], errs[1] = p2.Propose(ctx, paxos.Value("v2")) }()
	wg.Wait()

	// Both must succeed and agree on the same value.
	for i, err := range errs {
		if err != nil {
			t.Fatalf("proposer %d failed: %v", i, err)
		}
	}
	if string(results[0]) != string(results[1]) {
		t.Fatalf("agreement violated: got %q and %q", results[0], results[1])
	}
}

// --- Flexible Paxos quorum configs -------------------------------------------

func TestFlexiblePaxosConfigValidation(t *testing.T) {
	acceptors := []string{"a1", "a2", "a3", "a4", "a5"}

	// Valid: 4+2 > 5
	cfg := flexConfig(acceptors, 4, 2)
	if !cfg.Valid() {
		t.Error("expected config to be valid")
	}

	// Invalid: 2+2 = 4 not > 5
	invalid := flexConfig(acceptors, 2, 2)
	if invalid.Valid() {
		t.Error("expected config to be invalid")
	}

	// Edge case: Q1=5, Q2=1 (still valid: 5+1=6>5)
	edge := flexConfig(acceptors, 5, 1)
	if !edge.Valid() {
		t.Error("expected edge config to be valid")
	}
}

func TestFlexiblePaxosReadOptimised(t *testing.T) {
	// Q1=4 (large), Q2=2 (small) on 5 nodes.
	// Phase 2 only needs 2 acceptors → lower write latency.
	_, transport := newCluster(5)
	acceptors := []string{"a1", "a2", "a3", "a4", "a5"}
	cfg := flexConfig(acceptors, 4, 2)

	p, err := paxos.NewProposer("leader", cfg, transport)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	got, err := p.Propose(ctx, paxos.Value("flexible-value"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "flexible-value" {
		t.Fatalf("unexpected value: %q", got)
	}
}

func TestFlexiblePaxosWriteOptimised(t *testing.T) {
	// Q1=2 (small), Q2=4 (large) on 5 nodes.
	// Phase 1 only needs 2 acceptors → faster leader elections.
	_, transport := newCluster(5)
	acceptors := []string{"a1", "a2", "a3", "a4", "a5"}
	cfg := flexConfig(acceptors, 2, 4)

	p, err := paxos.NewProposer("leader", cfg, transport)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	got, err := p.Propose(ctx, paxos.Value("write-opt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "write-opt" {
		t.Fatalf("unexpected value: %q", got)
	}
}

// --- Fault tolerance ---------------------------------------------------------

func TestPaxosSurvivesMinorityPartition(t *testing.T) {
	_, transport := newCluster(5)
	cfg := classicConfig(5)

	// Partition 2 of 5 acceptors.
	transport.Partition("a4")
	transport.Partition("a5")

	p, _ := paxos.NewProposer("leader", cfg, transport)
	ctx := context.Background()
	got, err := p.Propose(ctx, paxos.Value("survives"))
	if err != nil {
		t.Fatalf("expected success with minority partition: %v", err)
	}
	if string(got) != "survives" {
		t.Fatalf("unexpected value: %q", got)
	}
}

func TestPaxosFailsMajorityPartition(t *testing.T) {
	_, transport := newCluster(5)
	cfg := classicConfig(5)

	// Partition 3 of 5 (majority unavailable).
	transport.Partition("a1")
	transport.Partition("a2")
	transport.Partition("a3")

	p, _ := paxos.NewProposer("leader", cfg, transport)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := p.Propose(ctx, paxos.Value("wont-work"))
	if err == nil {
		t.Fatal("expected failure with majority partitioned")
	}
}

func TestFlexiblePaxosPartitionTolerance(t *testing.T) {
	// Q1=4, Q2=2 on 5 nodes.
	// Can tolerate 1 acceptor down in Phase 1, 3 in Phase 2.
	_, transport := newCluster(5)
	acceptors := []string{"a1", "a2", "a3", "a4", "a5"}
	cfg := flexConfig(acceptors, 4, 2)

	transport.Partition("a5") // only 4 available — exactly Q1

	p, _ := paxos.NewProposer("leader", cfg, transport)
	ctx := context.Background()
	got, err := p.Propose(ctx, paxos.Value("flex-fault"))
	if err != nil {
		t.Fatalf("unexpected failure: %v", err)
	}
	if string(got) != "flex-fault" {
		t.Fatalf("unexpected value: %q", got)
	}
}

// --- Concurrent leaders contest ----------------------------------------------

func TestConcurrentLeadersAgree(t *testing.T) {
	_, transport := newCluster(5)
	cfg := classicConfig(5)

	const numLeaders = 10
	type result struct {
		val paxos.Value
		err error
	}
	ch := make(chan result, numLeaders)

	for i := 0; i < numLeaders; i++ {
		go func(i int) {
			p, _ := paxos.NewProposer(fmt.Sprintf("l%d", i), cfg, transport)
			v, err := p.Propose(context.Background(), paxos.Value(fmt.Sprintf("v%d", i)))
			ch <- result{v, err}
		}(i)
	}

	var chosen paxos.Value
	for i := 0; i < numLeaders; i++ {
		r := <-ch
		if r.err != nil {
			continue // ballot collisions cause retries; some may time out
		}
		if chosen == nil {
			chosen = r.val
		} else if string(chosen) != string(r.val) {
			t.Fatalf("agreement violated: %q vs %q", chosen, r.val)
		}
	}
	if chosen == nil {
		t.Fatal("no value was chosen")
	}
}
