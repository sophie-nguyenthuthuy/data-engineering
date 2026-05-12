// Package algebra defines the mathematical structures underlying CRDTs.
// A Join Semilattice (S, ⊔) is the core: associative, commutative, idempotent merge.
// Any type satisfying Ops[S] is guaranteed to converge under concurrent updates.
package algebra

// Ops captures the join-semilattice operations for type S.
// These three functions are all that's needed to prove convergence.
type Ops[S any] struct {
	// Join computes the least upper bound: s1 ⊔ s2
	// Must be: associative, commutative, idempotent
	Join func(a, b S) S

	// LessEq checks the induced partial order: a ≤ b iff a ⊔ b = b
	LessEq func(a, b S) bool

	// Bottom returns the identity element (⊥): ⊥ ⊔ s = s for all s
	Bottom func() S

	// Equal checks semantic equality (may differ from ==)
	Equal func(a, b S) bool
}

// Product[A, B] is the Cartesian product of two semilattices.
// Join is component-wise: (a1,b1) ⊔ (a2,b2) = (a1⊔a2, b1⊔b2)
// Convergence proof: if A and B converge independently, A×B converges.
type Product[A, B any] struct {
	First  A
	Second B
}

// ProductOps derives lattice ops for A×B from ops for A and B.
// This is the key composition theorem: Lattice(A) ∧ Lattice(B) → Lattice(A×B)
func ProductOps[A, B any](opsA Ops[A], opsB Ops[B]) Ops[Product[A, B]] {
	return Ops[Product[A, B]]{
		Join: func(x, y Product[A, B]) Product[A, B] {
			return Product[A, B]{
				First:  opsA.Join(x.First, y.First),
				Second: opsB.Join(x.Second, y.Second),
			}
		},
		LessEq: func(x, y Product[A, B]) bool {
			return opsA.LessEq(x.First, y.First) && opsB.LessEq(x.Second, y.Second)
		},
		Bottom: func() Product[A, B] {
			return Product[A, B]{First: opsA.Bottom(), Second: opsB.Bottom()}
		},
		Equal: func(x, y Product[A, B]) bool {
			return opsA.Equal(x.First, y.First) && opsB.Equal(x.Second, y.Second)
		},
	}
}

// MapOps derives lattice ops for map[K]V with pointwise join.
// Empty entries implicitly equal Bottom(). This enables sparse representation.
// Convergence proof: pointwise join on a lattice yields a lattice.
func MapOps[K comparable, V any](opsV Ops[V]) Ops[map[K]V] {
	return Ops[map[K]V]{
		Join: func(a, b map[K]V) map[K]V {
			result := make(map[K]V, max(len(a), len(b)))
			for k, v := range a {
				result[k] = v
			}
			for k, bv := range b {
				if av, ok := result[k]; ok {
					result[k] = opsV.Join(av, bv)
				} else {
					result[k] = bv
				}
			}
			return result
		},
		LessEq: func(a, b map[K]V) bool {
			for k, av := range a {
				bv, ok := b[k]
				if !ok {
					bv = opsV.Bottom()
				}
				if !opsV.LessEq(av, bv) {
					return false
				}
			}
			return true
		},
		Bottom: func() map[K]V {
			return make(map[K]V)
		},
		Equal: func(a, b map[K]V) bool {
			if len(a) != len(b) {
				return false
			}
			for k, av := range a {
				bv, ok := b[k]
				if !ok || !opsV.Equal(av, bv) {
					return false
				}
			}
			return true
		},
	}
}

// WithDefault wraps Ops[V] so missing map keys use bottom instead of zero value.
// This is the semantic that makes MapOps correct.
func (ops Ops[V]) Get(m map[string]V, key string) V {
	if v, ok := m[key]; ok {
		return v
	}
	return ops.Bottom()
}

// MaxUint64Ops is the lattice of uint64 under max, used by G-Counter.
var MaxUint64Ops = Ops[uint64]{
	Join:   func(a, b uint64) uint64 { return maxU64(a, b) },
	LessEq: func(a, b uint64) bool { return a <= b },
	Bottom: func() uint64 { return 0 },
	Equal:  func(a, b uint64) bool { return a == b },
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
