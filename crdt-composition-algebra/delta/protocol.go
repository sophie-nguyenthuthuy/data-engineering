// Package delta implements the delta-state CRDT synchronization protocol.
//
// Unlike op-based CRDTs (which require reliable broadcast) or state-based CRDTs
// (which require full state transfer), delta-state CRDTs send minimal "deltas"
// that are sufficient to bring a recipient up to date given their causal context.
//
// Protocol:
//  1. Sender computes delta(state, recipient_cc) = minimal state ≥ recipient's cc
//  2. Sender ships delta + sender's cc to recipient
//  3. Recipient merges delta into its state and updates its cc
//  4. Delta accumulator aggregates unacknowledged deltas for re-transmission
package delta

import (
	"sync"
	"time"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
)

// DeltaState wraps a CRDT state S with causal metadata.
type DeltaState[S any] struct {
	State S
	CC    causal.Context // causal context: what events this state subsumes
}

// DeltaExtractor computes the minimal delta to bring a node from fromCC to this state.
// Returns the delta state and its associated causal context.
type DeltaExtractor[S any] func(state DeltaState[S], fromCC causal.Context) DeltaState[S]

// Merger merges a delta into the current state.
type Merger[S any] func(current, delta DeltaState[S]) DeltaState[S]

// Message is what nodes send to each other in the delta sync protocol.
type Message[S any] struct {
	From      causal.NodeID
	To        causal.NodeID
	Delta     DeltaState[S]
	SenderCC  causal.Context
	Timestamp time.Time
	SeqNo     uint64
}

// Ack acknowledges receipt of deltas up to a given sequence number.
type Ack struct {
	From  causal.NodeID
	To    causal.NodeID
	SeqNo uint64
	CC    causal.Context // recipient's current causal context
}

// DeltaBuffer accumulates unacknowledged deltas for re-transmission.
// Deltas are kept until the recipient acknowledges them via their causal context.
type DeltaBuffer[S any] struct {
	mu      sync.Mutex
	entries []bufferedDelta[S]
	merger  Merger[S]
	bottom  func() S
}

type bufferedDelta[S any] struct {
	delta DeltaState[S]
	seqNo uint64
	at    time.Time
}

func NewDeltaBuffer[S any](merger Merger[S], bottom func() S) *DeltaBuffer[S] {
	return &DeltaBuffer[S]{merger: merger, bottom: bottom}
}

// Append adds a new delta to the buffer.
func (b *DeltaBuffer[S]) Append(delta DeltaState[S], seqNo uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = append(b.entries, bufferedDelta[S]{delta: delta, seqNo: seqNo, at: time.Now()})
}

// Prune removes deltas whose events are all subsumed by the given causal context.
func (b *DeltaBuffer[S]) Prune(cc causal.Context) {
	b.mu.Lock()
	defer b.mu.Unlock()
	kept := b.entries[:0]
	for _, e := range b.entries {
		if !e.delta.CC.LessEq(cc) {
			kept = append(kept, e)
		}
	}
	b.entries = kept
}

// Aggregate merges all buffered deltas into a single delta for transmission.
func (b *DeltaBuffer[S]) Aggregate() (DeltaState[S], uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.entries) == 0 {
		return DeltaState[S]{CC: causal.NewContext()}, 0
	}
	agg := b.entries[0].delta
	maxSeq := b.entries[0].seqNo
	for _, e := range b.entries[1:] {
		agg = b.merger(agg, e.delta)
		if e.seqNo > maxSeq {
			maxSeq = e.seqNo
		}
	}
	return agg, maxSeq
}

// SyncSession manages a synchronization session between two replicas.
// It implements the "join semilattice sync" protocol:
//  1. Exchange causal contexts
//  2. Compute minimal deltas
//  3. Send deltas
//  4. Acknowledge
type SyncSession[S any] struct {
	LocalID   causal.NodeID
	LocalCC   causal.Context
	LocalBuf  *DeltaBuffer[S]
	Extractor DeltaExtractor[S]
	Merger    Merger[S]
	seqNo     uint64
	mu        sync.Mutex
}

// PrepareSync prepares a delta message to send to the remote node.
// It takes the current local state and the remote's last known CC.
func (s *SyncSession[S]) PrepareSync(localState DeltaState[S], remoteCC causal.Context) Message[S] {
	s.mu.Lock()
	s.seqNo++
	seq := s.seqNo
	s.mu.Unlock()

	delta := s.Extractor(localState, remoteCC)
	return Message[S]{
		From:      s.LocalID,
		Delta:     delta,
		SenderCC:  localState.CC,
		Timestamp: time.Now(),
		SeqNo:     seq,
	}
}

// ApplyMessage merges an incoming delta message into the local state.
// Returns the updated state and an ack to send back.
func (s *SyncSession[S]) ApplyMessage(current DeltaState[S], msg Message[S]) (DeltaState[S], Ack) {
	newState := s.Merger(current, msg.Delta)
	return newState, Ack{
		From:  s.LocalID,
		To:    msg.From,
		SeqNo: msg.SeqNo,
		CC:    newState.CC,
	}
}
