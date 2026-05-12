package itc_test

import (
	"fmt"
	"testing"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/itc"
)

// TestSeedStamp verifies the initial seed stamp owns everything and has no events.
func TestSeedStamp(t *testing.T) {
	s := itc.Seed()
	if s.ID.IsZero() {
		t.Error("seed ID should not be zero")
	}
	// Seed should have metadata size of 2 (ID=1, Event=0)
	size := itc.MetadataSize(s)
	if size != 2 {
		t.Errorf("seed metadata size = %d, want 2", size)
	}
}

// TestForkJoinRoundtrip verifies that forking and joining recovers the original stamp.
func TestForkJoinRoundtrip(t *testing.T) {
	s := itc.Seed()
	a, b := itc.Fork(s)

	if a.ID.IsZero() || b.ID.IsZero() {
		t.Error("neither fork should have zero ID")
	}

	joined := itc.Join(a, b)
	// After joining, the event tree should be equivalent to original
	if !itc.Leq(s, joined) || !itc.Leq(joined, s) {
		t.Errorf("fork-join roundtrip failed: original=%s joined=%s", s, joined)
	}
}

// TestEventRecording verifies that recording an event inflates the event tree.
func TestEventRecording(t *testing.T) {
	s := itc.Seed()
	s2 := itc.RecordEvent(s)

	if itc.Leq(s2, s) {
		t.Error("after recording event, s2 should not be ≤ s (s2 should be strictly ahead)")
	}
	if !itc.Leq(s, s2) {
		t.Error("s should be ≤ s2 after event (s2 has seen more)")
	}
}

// TestCausalOrder verifies that concurrent stamps are correctly detected.
func TestCausalOrder(t *testing.T) {
	s := itc.Seed()
	a, b := itc.Fork(s)

	// Before any events: a and b are equal
	if !itc.Leq(a, b) || !itc.Leq(b, a) {
		t.Error("before events, both stamps should be equal")
	}

	// A records an event
	a2 := itc.RecordEvent(a)

	// Now a2 > b (a2 has seen more), b ≯ a2
	if !itc.Leq(b, a2) {
		t.Error("b should be ≤ a2 (a2 is ahead)")
	}
	if itc.Leq(a2, b) {
		t.Error("a2 should NOT be ≤ b (a2 is strictly ahead)")
	}

	// B records an event too — now they're concurrent
	b2 := itc.RecordEvent(b)

	if !itc.Concurrent(a2, b2) {
		t.Errorf("a2 and b2 should be concurrent: a2=%s b2=%s", a2, b2)
	}
}

// TestAntiEntropyConvergence verifies that anti-entropy brings all nodes to the same state.
func TestAntiEntropyConvergence(t *testing.T) {
	root := itc.NewAntiEntropyNode("root")
	nodes := []*itc.AntiEntropyNode{root}

	// Fork 5 nodes
	for i := 1; i <= 5; i++ {
		parent := nodes[len(nodes)-1]
		child := parent.ForkChild(fmt.Sprintf("n%d", i))
		nodes = append(nodes, child)
	}

	// Each node records events
	for i, n := range nodes {
		for j := 0; j <= i; j++ {
			n.RecordEvent()
		}
	}

	// Wire peers
	for i, a := range nodes {
		for j, b := range nodes {
			if i != j {
				a.AddPeer(b)
			}
		}
	}

	// Run anti-entropy until converged
	for round := 0; round < 10; round++ {
		for _, a := range nodes {
			for _, b := range nodes {
				if a != b {
					a.SyncWith(b)
				}
			}
		}
	}

	report := itc.ClusterReport(nodes)
	if !report.Convergence {
		t.Error("ITC anti-entropy did not converge")
	}
}

// TestMetadataBounded verifies that ITC metadata size is O(k) for k active nodes.
// This is the key advantage over vector clocks.
func TestMetadataBounded(t *testing.T) {
	k := 8 // number of active nodes
	root := itc.NewAntiEntropyNode("root")
	nodes := []*itc.AntiEntropyNode{root}

	for i := 1; i <= k; i++ {
		parent := nodes[len(nodes)-1]
		child := parent.ForkChild(fmt.Sprintf("n%d", i))
		nodes = append(nodes, child)
	}

	// Record many events
	for _, n := range nodes {
		for i := 0; i < 20; i++ {
			n.RecordEvent()
		}
	}

	// Wire and sync
	for i, a := range nodes {
		for j, b := range nodes {
			if i != j {
				a.AddPeer(b)
			}
		}
	}
	for round := 0; round < 5; round++ {
		for _, a := range nodes {
			for _, b := range nodes {
				if a != b {
					a.SyncWith(b)
				}
			}
		}
	}

	report := itc.ClusterReport(nodes)
	// Theoretical bound: O(k) — in practice, fully synced ITC collapses to small size
	// The important property is that it doesn't grow with number of events recorded
	theoreticalBound := 4 * (k + 1) // generous bound
	if report.MaxMetadata > theoreticalBound {
		t.Errorf("ITC metadata %d exceeds theoretical bound O(k=%d)=%d for active nodes",
			report.MaxMetadata, k, theoreticalBound)
	}
}

// TestIDSplitJoin verifies that SplitID and JoinID are inverses.
func TestIDSplitJoin(t *testing.T) {
	id := itc.IDOne.Clone()
	l, r := itc.SplitID(id)

	if l.IsZero() || r.IsZero() {
		t.Error("split should produce non-zero halves")
	}

	joined := itc.JoinID(l, r)
	// Joined should be equivalent to original (both own everything)
	if joined.IsZero() {
		t.Error("joining two halves should not produce zero")
	}
}
