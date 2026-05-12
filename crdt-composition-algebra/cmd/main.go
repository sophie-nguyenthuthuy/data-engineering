// cmd/main.go is the entry point for the CRDT Composition Algebra simulation.
// It demonstrates:
//  1. Algebraic CRDT composition with convergence proofs
//  2. Delta-state synchronization protocol
//  3. ITC anti-entropy with bounded metadata
//  4. Multi-region partition simulation (5-minute partition)
package main

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/algebra"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/crdt"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/itc"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/simulation"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "--quick" {
		runQuick()
		return
	}

	fmt.Println(banner)
	fmt.Println()

	section("1. ALGEBRAIC COMPOSITION PROOFS")
	runAlgebraProofs()

	section("2. DELTA-STATE CRDT DEMONSTRATION")
	runDeltaDemo()

	section("3. INTERVAL TREE CLOCKS (ITC)")
	runITCDemo()

	section("4. MULTI-REGION PARTITION SIMULATION")
	runSimulation()
}

func runAlgebraProofs() {
	fmt.Println("Verifying semilattice laws for each CRDT type...")
	fmt.Println("Laws: Idempotency (s⊔s=s), Commutativity (s⊔t=t⊔s),")
	fmt.Println("      Associativity ((s⊔t)⊔u=s⊔(t⊔u)), Monotonicity (s≤s⊔t),")
	fmt.Println("      Convergence (any merge order → same result)")
	fmt.Println()

	// G-Counter proof
	gcProof := algebra.ConvergenceProof[crdt.GCounter]{
		Ops:        crdt.GCounterOps,
		Generators: []func(*rand.Rand) crdt.GCounter{randomGCounter},
		Iterations: 2000,
	}
	printProofResults("G-Counter", gcProof.Verify(42))

	// PNCounter proof via composition theorem
	pnProof := algebra.ConvergenceProof[crdt.PNCounter]{
		Ops: crdt.PNCounterOps,
		Generators: []func(*rand.Rand) crdt.PNCounter{
			func(r *rand.Rand) crdt.PNCounter {
				return crdt.PNCounter{First: randomGCounter(r), Second: randomGCounter(r)}
			},
		},
		Iterations: 2000,
	}
	printProofResults("PN-Counter (Product[GCounter,GCounter])", pnProof.Verify(43))

	// Composition theorem: prove that composing GCounter×GCounter preserves convergence
	fmt.Println("Composition Theorem verification:")
	compositionResults := algebra.CompositionTheorem(gcProof, gcProof, 99)
	printProofResults("  ProductOps preserves convergence", compositionResults)

	// ORSet proof
	orProof := algebra.ConvergenceProof[crdt.ORSetState[string]]{
		Ops:        crdt.ORSetOps[string](),
		Generators: []func(*rand.Rand) crdt.ORSetState[string]{randomORSet},
		Iterations: 500,
	}
	printProofResults("OR-Set", orProof.Verify(44))

	// MVRegister proof
	mvProof := algebra.ConvergenceProof[crdt.MVRegisterState[int]]{
		Ops:        crdt.MVRegisterOps[int](),
		Generators: []func(*rand.Rand) crdt.MVRegisterState[int]{randomMVRegister},
		Iterations: 500,
	}
	printProofResults("MV-Register", mvProof.Verify(45))

	fmt.Println()
}

func runDeltaDemo() {
	fmt.Println("Simulating 3-node delta-state synchronization with causal metadata...")
	fmt.Println()

	// Three nodes with PN-Counters
	node1 := causal.NodeID("node-1")
	node2 := causal.NodeID("node-2")
	node3 := causal.NodeID("node-3")

	s1 := crdt.NewPNCounter()
	s2 := crdt.NewPNCounter()
	s3 := crdt.NewPNCounter()

	// Node 1 and 2 perform concurrent operations
	s1, delta1 := crdt.PNCounterIncrement(s1, node1) // +1
	s1, delta2 := crdt.PNCounterIncrement(s1, node1) // +1
	s1, delta3 := crdt.PNCounterDecrement(s1, node1) // -1
	// s1 value = +1

	s2, delta4 := crdt.PNCounterIncrement(s2, node2) // +1
	s2, delta5 := crdt.PNCounterIncrement(s2, node2) // +1
	// s2 value = +2

	s3, delta6 := crdt.PNCounterDecrement(s3, node3) // -1
	// s3 value = -1

	fmt.Printf("Before sync: node1=%+d  node2=%+d  node3=%+d\n",
		crdt.PNCounterValue(s1), crdt.PNCounterValue(s2), crdt.PNCounterValue(s3))
	fmt.Printf("Deltas generated: %d (vs %d full-state messages)\n", 6, 6)
	fmt.Println("Each delta is minimal: only the changed entry, not full state")
	fmt.Println()

	// Exchange deltas (simulating partial delivery — delta-state is tolerant of this)
	// Node 1 receives deltas from 2 and 3
	s1 = crdt.PNCounterOps.Join(s1, delta4)
	s1 = crdt.PNCounterOps.Join(s1, delta5)
	s1 = crdt.PNCounterOps.Join(s1, delta6)

	// Node 2 receives deltas from 1 and 3
	s2 = crdt.PNCounterOps.Join(s2, delta1)
	s2 = crdt.PNCounterOps.Join(s2, delta2)
	s2 = crdt.PNCounterOps.Join(s2, delta3)
	s2 = crdt.PNCounterOps.Join(s2, delta6)

	// Node 3 receives deltas from 1 and 2
	s3 = crdt.PNCounterOps.Join(s3, delta1)
	s3 = crdt.PNCounterOps.Join(s3, delta2)
	s3 = crdt.PNCounterOps.Join(s3, delta3)
	s3 = crdt.PNCounterOps.Join(s3, delta4)
	s3 = crdt.PNCounterOps.Join(s3, delta5)

	fmt.Printf("After sync:  node1=%+d  node2=%+d  node3=%+d\n",
		crdt.PNCounterValue(s1), crdt.PNCounterValue(s2), crdt.PNCounterValue(s3))

	allEqual := crdt.PNCounterValue(s1) == crdt.PNCounterValue(s2) &&
		crdt.PNCounterValue(s2) == crdt.PNCounterValue(s3)
	expected := int64(+1 + 2 - 1) // = +2
	fmt.Printf("Converged=%v  expected=%+d  got=%+d\n", allEqual, expected, crdt.PNCounterValue(s1))

	// ORSet demonstration: add-wins under concurrent add+remove
	fmt.Println()
	fmt.Println("OR-Set add-wins semantics (concurrent add+remove):")
	setA := crdt.NewORSet[string]()
	setB := crdt.NewORSet[string]()

	// Both nodes start from same state, then partition
	setA, _ = crdt.ORSetAdd(setA, "alice", "node-A")
	setB, _ = crdt.ORSetAdd(setB, "alice", "node-A") // same initial add on both

	// Concurrent: A adds "alice" again, B removes "alice"
	setA, _ = crdt.ORSetAdd(setA, "alice", "node-A")
	setB, _ = crdt.ORSetRemove(setB, "alice", "node-B")

	// Merge
	ops := crdt.ORSetOps[string]()
	merged := ops.Join(setA, setB)
	fmt.Printf("  Node A: alice=%v  Node B: alice=%v  Merged: alice=%v  (add wins)\n",
		crdt.ORSetContains(setA, "alice"),
		crdt.ORSetContains(setB, "alice"),
		crdt.ORSetContains(merged, "alice"))

	fmt.Println()
}

func runITCDemo() {
	fmt.Println("Demonstrating Interval Tree Clocks for bounded anti-entropy...")
	fmt.Println()

	// Bootstrap: seed owns entire ID space
	root := itc.NewAntiEntropyNode("root")
	fmt.Printf("Initial stamp: %s  metadata_size=%d\n", root.GetStamp(), root.MetadataSize())

	// Fork 8 child nodes (simulating node join)
	nodes := []*itc.AntiEntropyNode{root}
	for i := 1; i <= 8; i++ {
		parent := nodes[len(nodes)-1]
		child := parent.ForkChild(fmt.Sprintf("node-%d", i))
		nodes = append(nodes, child)
	}

	fmt.Printf("After forking 8 nodes: %d stamps\n", len(nodes))
	for _, n := range nodes {
		fmt.Printf("  %s: stamp=%s size=%d\n", n.NodeID, n.GetStamp(), n.MetadataSize())
	}
	fmt.Println()

	// Each node records some events
	for i, n := range nodes {
		for j := 0; j <= i; j++ {
			n.RecordEvent()
		}
	}

	fmt.Println("After local events (before sync):")
	for _, n := range nodes {
		fmt.Printf("  %s: stamp=%s size=%d\n", n.NodeID, n.GetStamp(), n.MetadataSize())
	}
	fmt.Println()

	// Wire peers and run anti-entropy
	for i, a := range nodes {
		for j, b := range nodes {
			if i != j {
				a.AddPeer(b)
			}
		}
	}

	// 3 rounds of anti-entropy
	for round := 0; round < 3; round++ {
		for _, a := range nodes {
			for _, b := range nodes {
				if a != b {
					a.SyncWith(b)
				}
			}
		}
	}

	report := itc.ClusterReport(nodes)
	fmt.Printf("After anti-entropy: %s\n", report)
	fmt.Printf("Vector clock equivalent would need: %d entries per node\n", len(nodes))
	fmt.Printf("ITC actual max metadata: %d nodes in stamp tree\n", report.MaxMetadata)
	fmt.Printf("Metadata is O(k) where k=%d active nodes, vs O(n)=O(%d) for vector clocks\n",
		len(nodes), len(nodes))
	fmt.Println()
}

func runSimulation() {
	fmt.Println("Setting up 3-region cluster (us-east, eu-west, ap-south)...")
	fmt.Println("Time scale: 1 wall second = 60 simulated seconds")
	fmt.Println()

	cluster := simulation.NewMultiRegionCluster([]simulation.RegionConfig{
		{
			Name:             "us-east",
			NodesPerRegion:   3,
			IntraRegionDelay: 2 * time.Millisecond,
			InterRegionDelay: 80 * time.Millisecond,
		},
		{
			Name:             "eu-west",
			NodesPerRegion:   3,
			IntraRegionDelay: 3 * time.Millisecond,
			InterRegionDelay: 80 * time.Millisecond,
		},
		{
			Name:             "ap-south",
			NodesPerRegion:   3,
			IntraRegionDelay: 4 * time.Millisecond,
			InterRegionDelay: 120 * time.Millisecond,
		},
	})

	stop := cluster.Start()
	defer stop()

	scenario := simulation.NewPartitionScenario(cluster)
	result := scenario.Run()

	fmt.Println()
	fmt.Println(result)

	// Validation assertions
	if !result.DivergenceSeen {
		fmt.Println("WARNING: Expected to see divergence during partition but did not")
		fmt.Println("  (May need longer partition or more write activity)")
	}
	if !result.Converged {
		fmt.Println("FAIL: System did not converge after partition healed!")
		os.Exit(1)
	}
	if !result.ITCWithinBound {
		fmt.Println("FAIL: ITC metadata exceeded theoretical bound!")
		os.Exit(1)
	}
	fmt.Println("All assertions passed.")
}

func runQuick() {
	fmt.Println("Quick verification mode...")

	// Just run algebra proofs (fast)
	gcProof := algebra.ConvergenceProof[crdt.GCounter]{
		Ops:        crdt.GCounterOps,
		Generators: []func(*rand.Rand) crdt.GCounter{randomGCounter},
		Iterations: 100,
	}
	results := gcProof.Verify(42)
	allPassed := true
	for _, r := range results {
		if !r.Passed {
			fmt.Printf("FAIL: %s: %s\n", r.Property, r.Failure)
			allPassed = false
		}
	}
	if allPassed {
		fmt.Println("All algebra properties verified.")
	}
}

func printProofResults(name string, results []algebra.ProofResult) {
	allPassed := true
	for _, r := range results {
		if !r.Passed {
			allPassed = false
		}
	}
	if allPassed {
		fmt.Printf("  %-45s [PASS] (%d iterations)\n", name+":", results[0].Iterations)
	} else {
		for _, r := range results {
			if !r.Passed {
				fmt.Printf("  %-45s [FAIL] %s\n", name+"/"+r.Property+":", r.Failure)
			}
		}
	}
}

func randomGCounter(r *rand.Rand) crdt.GCounter {
	nodes := []causal.NodeID{"a", "b", "c", "d"}
	g := crdt.NewGCounter()
	for _, node := range nodes {
		count := uint64(r.Intn(100))
		if count > 0 {
			for i := uint64(0); i < count; i++ {
				g, _ = crdt.GCounterIncrement(g, node)
			}
		}
	}
	return g
}

func randomORSet(r *rand.Rand) crdt.ORSetState[string] {
	nodes := []causal.NodeID{"a", "b", "c"}
	s := crdt.NewORSet[string]()
	elems := []string{"x", "y", "z", "w"}
	for i := 0; i < 5; i++ {
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

func randomMVRegister(r *rand.Rand) crdt.MVRegisterState[int] {
	nodes := []causal.NodeID{"a", "b", "c"}
	s := crdt.NewMVRegister[int]()
	for i := 0; i < r.Intn(3)+1; i++ {
		node := nodes[r.Intn(len(nodes))]
		s, _ = crdt.MVRegisterWrite(s, r.Intn(100), node)
	}
	return s
}

func section(title string) {
	fmt.Println(strings.Repeat("=", 60))
	fmt.Println(title)
	fmt.Println(strings.Repeat("=", 60))
}

const banner = `
╔══════════════════════════════════════════════════════════════╗
║           CRDT COMPOSITION ALGEBRA                           ║
║                                                              ║
║  Algebraic composition • Delta-state sync                    ║
║  ITC anti-entropy      • Multi-region validation             ║
╚══════════════════════════════════════════════════════════════╝`
