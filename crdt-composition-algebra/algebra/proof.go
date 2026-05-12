package algebra

import (
	"fmt"
	"math/rand"
)

// ConvergenceProof runs property-based tests to verify that Ops[S] forms a valid
// join semilattice. These properties are necessary and sufficient for CRDT convergence.
type ConvergenceProof[S any] struct {
	Ops        Ops[S]
	Generators []func(r *rand.Rand) S
	Iterations int
}

// ProofResult captures the outcome of each property check.
type ProofResult struct {
	Property   string
	Passed     bool
	Iterations int
	Failure    string
}

// Verify runs all three semilattice laws plus the convergence corollary.
func (p ConvergenceProof[S]) Verify(seed int64) []ProofResult {
	r := rand.New(rand.NewSource(seed))
	gen := func() S {
		g := p.Generators[r.Intn(len(p.Generators))]
		return g(r)
	}
	n := p.Iterations
	if n == 0 {
		n = 1000
	}

	results := []ProofResult{
		p.checkIdempotency(r, gen, n),
		p.checkCommutativity(r, gen, n),
		p.checkAssociativity(r, gen, n),
		p.checkMonotonicity(r, gen, n),
		p.checkConvergence(r, gen, n),
	}
	return results
}

// Idempotency: s ⊔ s = s
// This ensures re-delivery of the same update has no effect (exactly-once semantics for free).
func (p ConvergenceProof[S]) checkIdempotency(r *rand.Rand, gen func() S, n int) ProofResult {
	for i := 0; i < n; i++ {
		s := gen()
		joined := p.Ops.Join(s, s)
		if !p.Ops.Equal(joined, s) {
			return ProofResult{
				Property: "Idempotency (s ⊔ s = s)",
				Passed:   false,
				Failure:  fmt.Sprintf("iteration %d: Join(s,s) ≠ s", i),
			}
		}
	}
	return ProofResult{Property: "Idempotency (s ⊔ s = s)", Passed: true, Iterations: n}
}

// Commutativity: s1 ⊔ s2 = s2 ⊔ s1
// This ensures update order doesn't matter (concurrent updates converge regardless of delivery order).
func (p ConvergenceProof[S]) checkCommutativity(r *rand.Rand, gen func() S, n int) ProofResult {
	for i := 0; i < n; i++ {
		s1, s2 := gen(), gen()
		ab := p.Ops.Join(s1, s2)
		ba := p.Ops.Join(s2, s1)
		if !p.Ops.Equal(ab, ba) {
			return ProofResult{
				Property: "Commutativity (s1 ⊔ s2 = s2 ⊔ s1)",
				Passed:   false,
				Failure:  fmt.Sprintf("iteration %d: Join(a,b) ≠ Join(b,a)", i),
			}
		}
	}
	return ProofResult{Property: "Commutativity (s1 ⊔ s2 = s2 ⊔ s1)", Passed: true, Iterations: n}
}

// Associativity: (s1 ⊔ s2) ⊔ s3 = s1 ⊔ (s2 ⊔ s3)
// This ensures merge order doesn't matter (any merge tree produces the same result).
func (p ConvergenceProof[S]) checkAssociativity(r *rand.Rand, gen func() S, n int) ProofResult {
	for i := 0; i < n; i++ {
		s1, s2, s3 := gen(), gen(), gen()
		left := p.Ops.Join(p.Ops.Join(s1, s2), s3)
		right := p.Ops.Join(s1, p.Ops.Join(s2, s3))
		if !p.Ops.Equal(left, right) {
			return ProofResult{
				Property: "Associativity ((s1⊔s2)⊔s3 = s1⊔(s2⊔s3))",
				Passed:   false,
				Failure:  fmt.Sprintf("iteration %d: (a⊔b)⊔c ≠ a⊔(b⊔c)", i),
			}
		}
	}
	return ProofResult{Property: "Associativity ((s1⊔s2)⊔s3 = s1⊔(s2⊔s3))", Passed: true, Iterations: n}
}

// Monotonicity: s1 ≤ s1 ⊔ s2
// This ensures updates only move state "up" the lattice — no rollbacks.
func (p ConvergenceProof[S]) checkMonotonicity(r *rand.Rand, gen func() S, n int) ProofResult {
	for i := 0; i < n; i++ {
		s1, s2 := gen(), gen()
		merged := p.Ops.Join(s1, s2)
		if !p.Ops.LessEq(s1, merged) {
			return ProofResult{
				Property: "Monotonicity (s ≤ s ⊔ t)",
				Passed:   false,
				Failure:  fmt.Sprintf("iteration %d: s1 not ≤ Join(s1,s2)", i),
			}
		}
		if !p.Ops.LessEq(s2, merged) {
			return ProofResult{
				Property: "Monotonicity (s ≤ s ⊔ t)",
				Passed:   false,
				Failure:  fmt.Sprintf("iteration %d: s2 not ≤ Join(s1,s2)", i),
			}
		}
	}
	return ProofResult{Property: "Monotonicity (s ≤ s ⊔ t)", Passed: true, Iterations: n}
}

// Convergence corollary: any sequence of concurrent updates that are eventually delivered
// to all nodes will produce the same final state (Strong Eventual Consistency).
// We test this by simulating k concurrent histories and verifying they all merge to the same state.
func (p ConvergenceProof[S]) checkConvergence(r *rand.Rand, gen func() S, n int) ProofResult {
	for i := 0; i < n/10; i++ {
		// Generate 3-5 concurrent states (simulating partitioned nodes)
		k := 3 + r.Intn(3)
		states := make([]S, k)
		for j := range states {
			states[j] = gen()
		}

		// Merge in random orders and verify same result
		merged1 := mergeAll(p.Ops, states, r)
		merged2 := mergeAll(p.Ops, states, r)
		merged3 := mergeAll(p.Ops, states, r)

		if !p.Ops.Equal(merged1, merged2) || !p.Ops.Equal(merged2, merged3) {
			return ProofResult{
				Property: "Convergence (concurrent histories merge to same state)",
				Passed:   false,
				Failure:  fmt.Sprintf("iteration %d: different merge orders produced different results", i),
			}
		}
	}
	return ProofResult{Property: "Convergence (concurrent histories merge to same state)", Passed: true, Iterations: n / 10}
}

func mergeAll[S any](ops Ops[S], states []S, r *rand.Rand) S {
	// Shuffle and merge to test order independence
	perm := r.Perm(len(states))
	result := ops.Bottom()
	for _, i := range perm {
		result = ops.Join(result, states[i])
	}
	return result
}

// CompositionTheorem proves that ProductOps preserves convergence:
// if opsA and opsB are valid lattices, ProductOps(opsA, opsB) is also a valid lattice.
func CompositionTheorem[A, B any](
	proofA ConvergenceProof[A],
	proofB ConvergenceProof[B],
	seed int64,
) []ProofResult {
	r := rand.New(rand.NewSource(seed))
	productOps := ProductOps(proofA.Ops, proofB.Ops)

	genA := func() A {
		g := proofA.Generators[r.Intn(len(proofA.Generators))]
		return g(r)
	}
	genB := func() B {
		g := proofB.Generators[r.Intn(len(proofB.Generators))]
		return g(r)
	}
	genAB := func(r2 *rand.Rand) Product[A, B] {
		return Product[A, B]{First: genA(), Second: genB()}
	}

	proof := ConvergenceProof[Product[A, B]]{
		Ops:        productOps,
		Generators: []func(*rand.Rand) Product[A, B]{genAB},
		Iterations: 500,
	}

	results := proof.Verify(seed + 1)
	for i := range results {
		results[i].Property = "Composition: " + results[i].Property
	}
	return results
}
