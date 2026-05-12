package linearizability

import (
	"fmt"
	"strings"
)

// CheckResult summarises the outcome of a linearizability check.
type CheckResult struct {
	OK        bool
	Anomalies []Anomaly
}

func (r CheckResult) String() string {
	if r.OK {
		return "linearizable: OK"
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("NOT linearizable: %d anomaly(ies)\n", len(r.Anomalies)))
	for _, a := range r.Anomalies {
		sb.WriteString("  ")
		sb.WriteString(a.String())
		sb.WriteByte('\n')
	}
	return sb.String()
}

// AnomalyKind names the type of cycle detected.
type AnomalyKind string

const (
	// G1a: an aborted write appears to be read by another transaction.
	AnomalyG1a AnomalyKind = "G1a"
	// G1b: a transaction reads an intermediate (overwritten) value.
	AnomalyG1b AnomalyKind = "G1b"
	// G1c: a cycle in ww+wr dependencies.
	AnomalyG1c AnomalyKind = "G1c"
	// G2: a cycle involving at least one rw anti-dependency.
	AnomalyG2 AnomalyKind = "G2"
)

// Anomaly describes a detected consistency violation with its cycle.
type Anomaly struct {
	Kind  AnomalyKind
	Cycle []cycleEdge
}

func (a Anomaly) String() string {
	var parts []string
	for _, e := range a.Cycle {
		parts = append(parts, fmt.Sprintf("op%d -[%s]-> op%d", e.from, e.label, e.to))
	}
	return fmt.Sprintf("%s: %s", a.Kind, strings.Join(parts, ", "))
}

type cycleEdge struct {
	from, to int64
	label    string
}

// --- Dependency graph -----------------------------------------------------------

type edgeKind int

const (
	edgeWR edgeKind = iota // write-read: writer → reader
	edgeWW                  // write-write: earlier writer → later writer
	edgeRW                  // read-write anti-dep: reader → writer that overwrites
	edgeRT                  // real-time: op1.ReturnAt ≤ op2.InvokeAt → op1 must precede op2
)

func (k edgeKind) String() string {
	switch k {
	case edgeWR:
		return "wr"
	case edgeWW:
		return "ww"
	case edgeRW:
		return "rw"
	default:
		return "rt"
	}
}

type depEdge struct {
	to   int64
	kind edgeKind
}

type graph struct {
	// adjacency list: op.ID → []depEdge
	adj map[int64][]depEdge
	// node set
	nodes map[int64]bool
}

func newGraph() *graph {
	return &graph{
		adj:   make(map[int64][]depEdge),
		nodes: make(map[int64]bool),
	}
}

func (g *graph) addNode(id int64) { g.nodes[id] = true }

func (g *graph) addEdge(from, to int64, k edgeKind) {
	g.adj[from] = append(g.adj[from], depEdge{to: to, kind: k})
}

// --- Check entrypoint -----------------------------------------------------------

// CheckPerKey checks linearizability independently for each key in the history.
// Use this for multi-key KV stores where each key is backed by an independent
// consensus instance. Cross-key real-time ordering is NOT enforced.
func CheckPerKey(history []Operation) CheckResult {
	byKey := make(map[string][]Operation)
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	var anomalies []Anomaly
	for _, ops := range byKey {
		r := Check(ops)
		anomalies = append(anomalies, r.Anomalies...)
	}
	return CheckResult{OK: len(anomalies) == 0, Anomalies: anomalies}
}

// Check runs the Elle-style linearizability analysis on a history of
// single-key register operations.
//
// Algorithm overview:
//  1. Discard pending (never-returned) operations — they cannot be
//     definitively placed in the linearisation order.
//  2. For each key, sort writes chronologically to build the version order.
//  3. Add wr edges: each write W → every read R where R.Value == W.Value.
//  4. Add ww edges: consecutive writes on the same key in version order.
//  5. Add rw edges: for each read R of value v, add R → every write W that
//     writes a different value v' (anti-dependency, W must come after R).
//  6. Detect cycles using DFS (Tarjan's SCC algorithm).
//  7. Classify cycles by the edge types they contain.
func Check(history []Operation) CheckResult {
	// --- 1. Filter to completed operations -------------------------------------
	var completed []Operation
	for _, op := range history {
		if !op.ReturnAt.IsZero() && op.OK {
			completed = append(completed, op)
		}
	}

	// --- 2. Build per-key write version orders ---------------------------------
	// keyWrites[key] = list of write operations sorted by ReturnAt (proxy for
	// real-time order among non-overlapping ops; for concurrent writes we use
	// InvokeAt as a tiebreaker).
	keyWrites := make(map[string][]Operation)
	for _, op := range completed {
		if op.Kind == OpWrite {
			keyWrites[op.Key] = append(keyWrites[op.Key], op)
		}
	}
	for key := range keyWrites {
		sortByRealTime(keyWrites[key])
	}

	g := newGraph()
	for _, op := range completed {
		g.addNode(op.ID)
	}

	// Index: for each (key, value) → the write that produced it.
	type kv struct{ key, val string }
	writeIndex := make(map[kv][]int64)
	for _, op := range completed {
		if op.Kind == OpWrite {
			writeIndex[kv{op.Key, op.Value}] = append(writeIndex[kv{op.Key, op.Value}], op.ID)
		}
	}

	// --- 3. wr edges -----------------------------------------------------------
	for _, op := range completed {
		if op.Kind != OpRead {
			continue
		}
		for _, wID := range writeIndex[kv{op.Key, op.Value}] {
			if wID != op.ID {
				g.addEdge(wID, op.ID, edgeWR)
			}
		}
	}

	// --- 4. ww edges -----------------------------------------------------------
	// Only draw W1→W2 when W1 and W2 are strictly non-overlapping
	// (W1.ReturnAt ≤ W2.InvokeAt). For concurrent writes the real-time order
	// is determined by the consensus layer, not wall-clock; drawing a ww edge
	// based on ReturnAt alone would be incorrect and produce false positives.
	for _, writes := range keyWrites {
		for i := 0; i+1 < len(writes); i++ {
			w1, w2 := writes[i], writes[i+1]
			// Strictly sequential: w1 fully completed before w2 started.
			if !w1.ReturnAt.After(w2.InvokeAt) {
				g.addEdge(w1.ID, w2.ID, edgeWW)
			}
		}
	}

	// --- 5. rw anti-dependency edges -------------------------------------------
	// For a read R that observes value v_r (written by W_r at version-order
	// position k), only writes W at positions > k "should have been visible"
	// to R if they preceded R. Writes at positions ≤ k were already overwritten
	// by W_r, so R correctly ignores them — no anti-dep needed there.
	//
	// If v_r == "" (initial / deleted value), all writes come after position -1,
	// so every write creates an rw edge (any write W that precedes R should have
	// been visible).
	writePos := make(map[kv]int) // (key, value) → index in sorted write list
	for key, writes := range keyWrites {
		for i, w := range writes {
			writePos[kv{key, w.Value}] = i
		}
	}

	for _, op := range completed {
		if op.Kind != OpRead {
			continue
		}
		observedPos := -1 // initial value (nothing written yet)
		if op.Value != "" {
			if p, ok := writePos[kv{op.Key, op.Value}]; ok {
				observedPos = p
			}
		}
		for i, w := range keyWrites[op.Key] {
			if i > observedPos && w.ID != op.ID {
				g.addEdge(op.ID, w.ID, edgeRW)
			}
		}
	}

	// --- 6. Real-time order edges ----------------------------------------------
	// If op1 fully completes before op2 starts (op1.ReturnAt ≤ op2.InvokeAt),
	// op1 must appear before op2 in any valid linearization. An rw anti-dep
	// that contradicts this real-time constraint reveals a stale read.
	for i, op1 := range completed {
		for j, op2 := range completed {
			if i == j {
				continue
			}
			if !op1.ReturnAt.After(op2.InvokeAt) {
				g.addEdge(op1.ID, op2.ID, edgeRT)
			}
		}
	}

	// --- 7. Cycle detection (Tarjan SCC) ---------------------------------------
	cycles := findCycles(g, completed)
	if len(cycles) == 0 {
		return CheckResult{OK: true}
	}

	// --- 8. Classify -----------------------------------------------------------
	var anomalies []Anomaly
	for _, cycle := range cycles {
		anomalies = append(anomalies, classifyCycle(cycle))
	}
	return CheckResult{OK: false, Anomalies: anomalies}
}

// sortByRealTime sorts ops by ReturnAt time (invokeAt as secondary key).
func sortByRealTime(ops []Operation) {
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0; j-- {
			a, b := ops[j-1], ops[j]
			if a.ReturnAt.After(b.ReturnAt) ||
				(a.ReturnAt.Equal(b.ReturnAt) && a.InvokeAt.After(b.InvokeAt)) {
				ops[j-1], ops[j] = ops[j], ops[j-1]
			} else {
				break
			}
		}
	}
}

// --- Tarjan SCC -----------------------------------------------------------------

type tarjanState struct {
	g       *graph
	index   map[int64]int
	lowlink map[int64]int
	onStack map[int64]bool
	stack   []int64
	counter int
	sccs    [][]int64
}

func findCycles(g *graph, ops []Operation) [][]cycleEdge {
	ts := &tarjanState{
		g:       g,
		index:   make(map[int64]int),
		lowlink: make(map[int64]int),
		onStack: make(map[int64]bool),
	}
	for _, op := range ops {
		if _, visited := ts.index[op.ID]; !visited {
			ts.strongConnect(op.ID)
		}
	}
	// Filter out trivial SCCs (single node, no self-loop).
	var cycles [][]cycleEdge
	for _, scc := range ts.sccs {
		if len(scc) > 1 {
			cycles = append(cycles, extractCycle(g, scc))
		}
	}
	return cycles
}

func (ts *tarjanState) strongConnect(v int64) {
	ts.index[v] = ts.counter
	ts.lowlink[v] = ts.counter
	ts.counter++
	ts.stack = append(ts.stack, v)
	ts.onStack[v] = true

	for _, e := range ts.g.adj[v] {
		w := e.to
		if _, visited := ts.index[w]; !visited {
			ts.strongConnect(w)
			if ts.lowlink[w] < ts.lowlink[v] {
				ts.lowlink[v] = ts.lowlink[w]
			}
		} else if ts.onStack[w] {
			if ts.index[w] < ts.lowlink[v] {
				ts.lowlink[v] = ts.index[w]
			}
		}
	}

	if ts.lowlink[v] == ts.index[v] {
		var scc []int64
		for {
			w := ts.stack[len(ts.stack)-1]
			ts.stack = ts.stack[:len(ts.stack)-1]
			ts.onStack[w] = false
			scc = append(scc, w)
			if w == v {
				break
			}
		}
		ts.sccs = append(ts.sccs, scc)
	}
}

// extractCycle recovers one cycle from an SCC by walking the adjacency list.
func extractCycle(g *graph, scc []int64) []cycleEdge {
	inSCC := make(map[int64]bool, len(scc))
	for _, id := range scc {
		inSCC[id] = true
	}
	var cycle []cycleEdge
	visited := make(map[int64]bool)
	start := scc[0]
	cur := start
	for {
		visited[cur] = true
		moved := false
		for _, e := range g.adj[cur] {
			if inSCC[e.to] && (!visited[e.to] || e.to == start) {
				cycle = append(cycle, cycleEdge{from: cur, to: e.to, label: e.kind.String()})
				if e.to == start {
					return cycle
				}
				cur = e.to
				moved = true
				break
			}
		}
		if !moved {
			break
		}
	}
	return cycle
}

// classifyCycle determines the anomaly type from the edge labels in a cycle.
func classifyCycle(cycle []cycleEdge) Anomaly {
	hasRW := false
	for _, e := range cycle {
		if e.label == "rw" {
			hasRW = true
			break
		}
	}
	kind := AnomalyG1c
	if hasRW {
		kind = AnomalyG2
	}
	return Anomaly{Kind: kind, Cycle: cycle}
}
