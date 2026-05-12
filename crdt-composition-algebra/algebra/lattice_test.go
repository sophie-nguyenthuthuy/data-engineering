package algebra_test

import (
	"math/rand"
	"testing"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/algebra"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/crdt"
)

// TestGCounterLaws verifies all semilattice laws for GCounter.
func TestGCounterLaws(t *testing.T) {
	proof := algebra.ConvergenceProof[crdt.GCounter]{
		Ops:        crdt.GCounterOps,
		Generators: []func(*rand.Rand) crdt.GCounter{randomGC},
		Iterations: 1000,
	}
	for _, r := range proof.Verify(42) {
		if !r.Passed {
			t.Errorf("GCounter %s: %s", r.Property, r.Failure)
		}
	}
}

// TestPNCounterLaws verifies all semilattice laws for PNCounter.
func TestPNCounterLaws(t *testing.T) {
	proof := algebra.ConvergenceProof[crdt.PNCounter]{
		Ops: crdt.PNCounterOps,
		Generators: []func(*rand.Rand) crdt.PNCounter{
			func(r *rand.Rand) crdt.PNCounter {
				return crdt.PNCounter{First: randomGC(r), Second: randomGC(r)}
			},
		},
		Iterations: 1000,
	}
	for _, r := range proof.Verify(43) {
		if !r.Passed {
			t.Errorf("PNCounter %s: %s", r.Property, r.Failure)
		}
	}
}

// TestCompositionTheorem proves that ProductOps preserves all lattice laws.
func TestCompositionTheorem(t *testing.T) {
	gcProof := algebra.ConvergenceProof[crdt.GCounter]{
		Ops:        crdt.GCounterOps,
		Generators: []func(*rand.Rand) crdt.GCounter{randomGC},
		Iterations: 500,
	}
	for _, r := range algebra.CompositionTheorem(gcProof, gcProof, 99) {
		if !r.Passed {
			t.Errorf("Composition: %s: %s", r.Property, r.Failure)
		}
	}
}

// TestMapOpsLaws verifies MapOps forms a valid lattice.
func TestMapOpsLaws(t *testing.T) {
	mapOps := algebra.MapOps[string, uint64](algebra.MaxUint64Ops)
	proof := algebra.ConvergenceProof[map[string]uint64]{
		Ops: mapOps,
		Generators: []func(*rand.Rand) map[string]uint64{
			func(r *rand.Rand) map[string]uint64 {
				m := make(map[string]uint64)
				keys := []string{"a", "b", "c"}
				for _, k := range keys {
					if r.Intn(2) == 0 {
						m[k] = uint64(r.Intn(100))
					}
				}
				return m
			},
		},
		Iterations: 500,
	}
	for _, r := range proof.Verify(77) {
		if !r.Passed {
			t.Errorf("MapOps %s: %s", r.Property, r.Failure)
		}
	}
}

// TestORSetLaws verifies all semilattice laws for ORSet.
func TestORSetLaws(t *testing.T) {
	proof := algebra.ConvergenceProof[crdt.ORSetState[string]]{
		Ops:        crdt.ORSetOps[string](),
		Generators: []func(*rand.Rand) crdt.ORSetState[string]{randomORSet},
		Iterations: 300,
	}
	for _, r := range proof.Verify(55) {
		if !r.Passed {
			t.Errorf("ORSet %s: %s", r.Property, r.Failure)
		}
	}
}

// TestMVRegisterLaws verifies all semilattice laws for MVRegister.
func TestMVRegisterLaws(t *testing.T) {
	proof := algebra.ConvergenceProof[crdt.MVRegisterState[int]]{
		Ops:        crdt.MVRegisterOps[int](),
		Generators: []func(*rand.Rand) crdt.MVRegisterState[int]{randomMVR},
		Iterations: 300,
	}
	for _, r := range proof.Verify(66) {
		if !r.Passed {
			t.Errorf("MVRegister %s: %s", r.Property, r.Failure)
		}
	}
}

// TestPNCounterDeltaConvergence verifies that delta-based sync converges.
func TestPNCounterDeltaConvergence(t *testing.T) {
	node1 := causal.NodeID("n1")
	node2 := causal.NodeID("n2")

	s1 := crdt.NewPNCounter()
	s2 := crdt.NewPNCounter()

	var deltas []crdt.PNCounter
	for i := 0; i < 5; i++ {
		var d crdt.PNCounter
		s1, d = crdt.PNCounterIncrement(s1, node1)
		deltas = append(deltas, d)
	}
	for i := 0; i < 3; i++ {
		var d crdt.PNCounter
		s2, d = crdt.PNCounterIncrement(s2, node2)
		deltas = append(deltas, d)
	}
	var d crdt.PNCounter
	s2, d = crdt.PNCounterDecrement(s2, node2)
	deltas = append(deltas, d)

	// Exchange all deltas (out of order)
	for _, delta := range deltas {
		s1 = crdt.PNCounterOps.Join(s1, delta)
		s2 = crdt.PNCounterOps.Join(s2, delta)
	}

	v1 := crdt.PNCounterValue(s1)
	v2 := crdt.PNCounterValue(s2)
	if v1 != v2 {
		t.Errorf("Expected convergence: s1=%d s2=%d", v1, v2)
	}
	expected := int64(5 + 3 - 1) // 7
	if v1 != expected {
		t.Errorf("Expected value %d, got %d", expected, v1)
	}
}

// TestORSetAddWins verifies that concurrent add beats concurrent remove.
func TestORSetAddWins(t *testing.T) {
	ops := crdt.ORSetOps[string]()

	setA := crdt.NewORSet[string]()
	setB := crdt.NewORSet[string]()

	// Both nodes add "x"
	setA, _ = crdt.ORSetAdd(setA, "x", "a")
	setB, _ = crdt.ORSetAdd(setB, "x", "a")

	// Concurrent: A adds "x" again, B removes "x"
	setA, _ = crdt.ORSetAdd(setA, "x", "a")
	setB, _ = crdt.ORSetRemove(setB, "x", "b")

	// Merge both ways
	mergedAB := ops.Join(setA, setB)
	mergedBA := ops.Join(setB, setA)

	if !crdt.ORSetContains(mergedAB, "x") {
		t.Error("add-wins violated: x should be present after concurrent add+remove (A→B)")
	}
	if !crdt.ORSetContains(mergedBA, "x") {
		t.Error("add-wins violated: x should be present after concurrent add+remove (B→A)")
	}
}

// TestMVRegisterConcurrentWrites verifies that concurrent writes are all preserved.
func TestMVRegisterConcurrentWrites(t *testing.T) {
	s1 := crdt.NewMVRegister[string]()
	s2 := crdt.NewMVRegister[string]()

	s1, _ = crdt.MVRegisterWrite(s1, "hello", "node-1")
	s2, _ = crdt.MVRegisterWrite(s2, "world", "node-2")

	ops := crdt.MVRegisterOps[string]()
	merged := ops.Join(s1, s2)
	values := crdt.MVRegisterRead(merged)

	if len(values) != 2 {
		t.Errorf("Expected 2 concurrent values, got %d: %v", len(values), values)
	}
}

func randomGC(r *rand.Rand) crdt.GCounter {
	g := crdt.NewGCounter()
	nodes := []causal.NodeID{"a", "b", "c"}
	for _, node := range nodes {
		n := r.Intn(20)
		for i := 0; i < n; i++ {
			g, _ = crdt.GCounterIncrement(g, node)
		}
	}
	return g
}

func randomORSet(r *rand.Rand) crdt.ORSetState[string] {
	s := crdt.NewORSet[string]()
	nodes := []causal.NodeID{"a", "b"}
	elems := []string{"x", "y", "z"}
	for i := 0; i < 4; i++ {
		node := nodes[r.Intn(len(nodes))]
		elem := elems[r.Intn(len(elems))]
		if r.Intn(2) == 0 {
			s, _ = crdt.ORSetAdd(s, elem, node)
		} else {
			s, _ = crdt.ORSetRemove(s, elem, node)
		}
	}
	return s
}

func randomMVR(r *rand.Rand) crdt.MVRegisterState[int] {
	s := crdt.NewMVRegister[int]()
	nodes := []causal.NodeID{"a", "b"}
	for i := 0; i < r.Intn(3)+1; i++ {
		s, _ = crdt.MVRegisterWrite(s, r.Intn(100), nodes[r.Intn(len(nodes))])
	}
	return s
}
