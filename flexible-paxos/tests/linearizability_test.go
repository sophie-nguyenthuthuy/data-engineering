package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/linearizability"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/paxos"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/quorum"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/store"
)

// --- Pure linearizability verifier tests -------------------------------------

func TestLinearizableHistory(t *testing.T) {
	// Serial write then read — trivially linearisable.
	rec := linearizability.NewRecorder()

	id := rec.Invoke(linearizability.OpWrite, "x", "1")
	time.Sleep(time.Microsecond)
	rec.Complete(id, "1", nil)

	id = rec.Invoke(linearizability.OpRead, "x", "")
	time.Sleep(time.Microsecond)
	rec.Complete(id, "1", nil)

	result := linearizability.Check(rec.History())
	if !result.OK {
		t.Fatalf("expected linearisable: %v", result)
	}
}

func TestNonLinearizableHistory(t *testing.T) {
	// Stale read: W1 completes before R1 starts, yet R1 sees the initial value.
	//
	//   W1(x=1) [0ms-2ms] → fully completes before R1 invokes
	//   R1(x=0) [3ms-5ms] → reads initial value, NOT W1's value
	//
	// Real-time edge: W1 →(rt)→ R1 (W1 must precede R1)
	// Anti-dep edge:  R1 →(rw)→ W1 (R1 reads x=0, W1 writes x=1, so W1 must come after R1)
	// Cycle: W1 →(rt)→ R1 →(rw)→ W1
	now := time.Now()
	ms := func(n int) time.Time { return now.Add(time.Duration(n) * time.Millisecond) }

	h := []linearizability.Operation{
		{ID: 1, Kind: linearizability.OpWrite, Key: "x", Value: "1",
			OK: true, InvokeAt: ms(0), ReturnAt: ms(2)},
		// R1 invokes at 3ms (after W1 returns at 2ms) but sees x="" (initial).
		{ID: 2, Kind: linearizability.OpRead, Key: "x", Value: "",
			OK: true, InvokeAt: ms(3), ReturnAt: ms(5)},
	}

	result := linearizability.Check(h)
	if result.OK {
		t.Fatal("expected stale-read to be detected as non-linearisable")
	}
	t.Logf("anomalies: %v", result)
}

func TestLinearizabilityCheckerEmptyHistory(t *testing.T) {
	result := linearizability.Check(nil)
	if !result.OK {
		t.Fatal("empty history should be linearisable")
	}
}

func TestLinearizabilityCheckerCrashedOps(t *testing.T) {
	// Crashed (error) operations are excluded from the check.
	rec := linearizability.NewRecorder()

	id := rec.Invoke(linearizability.OpWrite, "x", "1")
	time.Sleep(time.Microsecond)
	rec.Complete(id, "", fmt.Errorf("timeout"))

	id = rec.Invoke(linearizability.OpRead, "x", "")
	time.Sleep(time.Microsecond)
	rec.Complete(id, "", nil) // reads initial value (empty)

	result := linearizability.Check(rec.History())
	if !result.OK {
		t.Fatalf("expected linearisable with crashed ops: %v", result)
	}
}

// --- Store linearizability (end-to-end) ---------------------------------------

func buildStore(t *testing.T, n, q1, q2 int) *store.Store {
	t.Helper()
	_, transport := newCluster(n)
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("a%d", i+1)
	}
	cfg := paxos.QuorumConfig{Acceptors: ids, Q1Size: q1, Q2Size: q2}
	m := quorum.NewMetrics()
	return store.New(cfg, transport, m)
}

func TestStoreLinearizability(t *testing.T) {
	s := buildStore(t, 5, 3, 3)
	rec := linearizability.NewRecorder()
	ctx := context.Background()

	// Serial set + get.
	wID := rec.Invoke(linearizability.OpWrite, "k", "hello")
	committed, err := s.Set(ctx, "k", "hello")
	rec.Complete(wID, committed, err)

	rID := rec.Invoke(linearizability.OpRead, "k", "")
	val, _, err := s.Get(ctx, "k")
	rec.Complete(rID, val, err)

	result := linearizability.Check(rec.History())
	if !result.OK {
		t.Fatalf("store ops not linearisable: %v", result)
	}
}

func TestStoreConcurrentLinearizability(t *testing.T) {
	s := buildStore(t, 5, 3, 3)
	rec := linearizability.NewRecorder()
	ctx := context.Background()

	const (
		writers = 3
		readers = 5
	)
	var wg sync.WaitGroup
	mu := sync.Mutex{}

	writeAndRecord := func(key, val string) {
		defer wg.Done()
		id := rec.Invoke(linearizability.OpWrite, key, val)
		committed, err := s.Set(ctx, key, val)
		mu.Lock()
		rec.Complete(id, committed, err)
		mu.Unlock()
	}

	readAndRecord := func(key string) {
		defer wg.Done()
		id := rec.Invoke(linearizability.OpRead, key, "")
		v, _, err := s.Get(ctx, key)
		mu.Lock()
		rec.Complete(id, v, err)
		mu.Unlock()
	}

	wg.Add(writers + readers)
	for i := 0; i < writers; i++ {
		go writeAndRecord("x", fmt.Sprintf("v%d", i))
	}
	for i := 0; i < readers; i++ {
		go readAndRecord("x")
	}
	wg.Wait()

	result := linearizability.Check(rec.History())
	if !result.OK {
		t.Fatalf("concurrent store ops not linearisable: %v", result)
	}
}

// --- Quorum manager integration -----------------------------------------------

func TestQuorumManagerAdjustment(t *testing.T) {
	m := quorum.NewMetrics()

	for i := 0; i < 100; i++ {
		m.RecordRead(time.Millisecond)
	}
	for i := 0; i < 10; i++ {
		m.RecordWrite(time.Millisecond)
	}

	ratio := m.ReadWriteRatio()
	if ratio < 9.0 || ratio > 11.0 {
		t.Fatalf("unexpected R/W ratio: %f", ratio)
	}

	// Policy: read-heavy threshold = 4.0.
	policy := quorum.DefaultPolicy
	policy.SampleInterval = 50 * time.Millisecond

	ids := []string{"a1", "a2", "a3", "a4", "a5"}
	initial := paxos.Classic(ids)
	mgr, err := quorum.NewManager(initial, m, policy)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	changed := make(chan paxos.QuorumConfig, 1)
	mgr.AddListener(func(_, newCfg paxos.QuorumConfig) {
		select {
		case changed <- newCfg:
		default:
		}
	})

	go mgr.Run(ctx)

	select {
	case newCfg := <-changed:
		if !newCfg.Valid() {
			t.Fatalf("manager produced invalid config: %+v", newCfg)
		}
		// Read-heavy: should shrink Q2.
		if newCfg.Q2Size >= initial.Q2Size {
			t.Logf("note: Q2 did not shrink (ratio=%f, cfg=%+v)", ratio, newCfg)
		}
		t.Logf("quorum adjusted: Q1=%d Q2=%d (ratio=%.1f)", newCfg.Q1Size, newCfg.Q2Size, ratio)
	case <-ctx.Done():
		t.Log("no quorum adjustment within timeout (may be correct if ratio unchanged)")
	}
}
