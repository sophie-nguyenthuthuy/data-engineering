// Package linearizability implements an Elle-inspired linearizability checker
// for key-value stores.
//
// # Background
//
// Elle (Kingsbury & Alvaro, 2021) detects consistency anomalies by building a
// dependency graph over transactions and checking for cycles. Each cycle
// corresponds to a specific consistency violation.
//
// For a single-key register the relevant graph edges are:
//
//	wr (write-read)   : T_w → T_r  if T_w writes x=v and T_r reads x=v
//	ww (write-write)  : T1  → T2   if T1 writes x=v1, T2 writes x=v2, and
//	                                T2's write is the one T_r observes (v2 overrides v1)
//	rw (anti-dep)     : T_r → T_w  if T_r reads x=v and T_w writes x=v' ≠ v
//	                                (T_w must come after T_r in real time)
//
// A cycle in {wr,ww,rw} edges constitutes a G1 anomaly (not linearisable).
//
// # Usage
//
//	rec := linearizability.NewRecorder()
//	opID := rec.Invoke(OpWrite, "k", "v")
//	// ... perform actual operation ...
//	rec.Complete(opID, "ok", nil)
//
//	result := linearizability.Check(rec.History())
//	if !result.OK { ... }
package linearizability

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// OpKind distinguishes reads from writes in a history.
type OpKind int

const (
	OpRead  OpKind = iota
	OpWrite        // includes delete (value = "")
)

func (k OpKind) String() string {
	if k == OpRead {
		return "read"
	}
	return "write"
}

// Operation is a single read or write on a key.
type Operation struct {
	ID       int64
	Kind     OpKind
	Key      string
	// Value written (for writes) or value observed (for reads).
	// Empty string represents a missing/deleted key.
	Value    string
	// OK=false means the operation returned an error (crash, timeout).
	// Crashed operations are treated conservatively: they may or may not
	// have taken effect.
	OK       bool
	InvokeAt time.Time
	ReturnAt time.Time
}

var opCounter atomic.Int64

// Recorder captures a concurrent history of operations for later analysis.
type Recorder struct {
	mu  sync.Mutex
	ops map[int64]*Operation
}

func NewRecorder() *Recorder {
	return &Recorder{ops: make(map[int64]*Operation)}
}

// Invoke records the start of an operation and returns its unique ID.
func (r *Recorder) Invoke(kind OpKind, key, value string) int64 {
	id := opCounter.Add(1)
	r.mu.Lock()
	r.ops[id] = &Operation{
		ID:       id,
		Kind:     kind,
		Key:      key,
		Value:    value,
		InvokeAt: time.Now(),
	}
	r.mu.Unlock()
	return id
}

// Complete records the end of operation id.  value is the observed value for
// reads; ok=false marks a crash/error.
func (r *Recorder) Complete(id int64, observedValue string, err error) {
	r.mu.Lock()
	op, exists := r.ops[id]
	if exists {
		op.ReturnAt = time.Now()
		op.OK = err == nil
		if op.Kind == OpRead {
			op.Value = observedValue
		}
	}
	r.mu.Unlock()
}

// History returns all recorded operations, sorted by invocation time.
func (r *Recorder) History() []Operation {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Operation, 0, len(r.ops))
	for _, op := range r.ops {
		out = append(out, *op)
	}
	// sort by InvokeAt for determinism
	sortOps(out)
	return out
}

func sortOps(ops []Operation) {
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0 && ops[j].InvokeAt.Before(ops[j-1].InvokeAt); j-- {
			ops[j], ops[j-1] = ops[j-1], ops[j]
		}
	}
}

// --- Pretty-print ---------------------------------------------------------------

func (op Operation) String() string {
	state := "ok"
	if !op.OK {
		state = "err"
	}
	if op.ReturnAt.IsZero() {
		state = "pending"
	}
	return fmt.Sprintf("[%d] %s(%s)=%q @%v (%s)",
		op.ID, op.Kind, op.Key, op.Value,
		op.InvokeAt.Format("15:04:05.000"), state)
}
