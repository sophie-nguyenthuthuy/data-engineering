// Package store provides a linearisable key-value metadata store built on top
// of Flexible Paxos.
//
// Each write runs a full Paxos round; each read either runs Phase 1 (strong
// read) or returns from a local cache (weak read, not linearisable).
//
// The quorum manager adjusts Q1/Q2 based on the observed R/W ratio. If the
// workload is read-heavy (ratio > policy.ReadHeavyThreshold) the manager
// shrinks Q2, reducing write-path latency. If write-heavy, it shrinks Q1.
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/paxos"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/quorum"
)

// Op tags the type of KV operation encoded in a Paxos value.
type OpType string

const (
	OpSet    OpType = "set"
	OpDelete OpType = "del"
	OpNoop   OpType = "nop"
)

// Op is the payload stored in each Paxos slot.
type Op struct {
	Type  OpType `json:"t"`
	Key   string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
	// Nonce makes each proposal unique even for identical writes.
	Nonce string `json:"n"`
}

func encodeOp(op Op) (paxos.Value, error) {
	return json.Marshal(op)
}

func decodeOp(v paxos.Value) (Op, error) {
	var op Op
	return op, json.Unmarshal(v, &op)
}

// Store is a linearisable KV store.
//
// Each key is backed by its own Paxos instance (a separate ballot-space).
// This avoids the "slot contention" problem that arises when multiple keys
// compete for a single Paxos slot: a write to key A would otherwise appear
// to commit a write to key B (the value that happened to win the slot), which
// makes the per-key history uninterpretable.
type Store struct {
	mu       sync.Mutex
	perKey   map[string]*keyStore // one Paxos instance per key
	cfg      paxos.QuorumConfig
	transport paxos.Transport
	metrics  *quorum.Metrics
}

// keyStore owns one Paxos proposer for a single key.
type keyStore struct {
	mu       sync.RWMutex
	value    string
	proposer *paxos.Proposer
}

// New creates a Store backed by per-key Paxos proposers that share the same
// acceptors and quorum configuration.
func New(cfg paxos.QuorumConfig, transport paxos.Transport, metrics *quorum.Metrics) *Store {
	return &Store{
		perKey:    make(map[string]*keyStore),
		cfg:       cfg,
		transport: transport,
		metrics:   metrics,
	}
}

func (s *Store) keyInstance(key string) *keyStore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ks, ok := s.perKey[key]; ok {
		return ks
	}
	p, _ := paxos.NewProposer("leader-"+key, s.cfg, s.transport)
	ks := &keyStore{proposer: p}
	s.perKey[key] = ks
	return ks
}

// Set durably writes key=value via a Paxos round.
// Returns the value that was actually committed (which may differ from the
// requested value if a prior proposal was already chosen for this slot).
func (s *Store) Set(ctx context.Context, key, value string) (string, error) {
	start := time.Now()
	ks := s.keyInstance(key)

	op := Op{Type: OpSet, Key: key, Value: value, Nonce: fmt.Sprintf("%d", time.Now().UnixNano())}
	payload, err := encodeOp(op)
	if err != nil {
		return "", err
	}

	chosen, err := ks.proposer.Propose(ctx, payload)
	if err != nil {
		return "", err
	}

	committedOp, err := decodeOp(chosen)
	if err != nil {
		return "", err
	}

	ks.mu.Lock()
	if committedOp.Type == OpSet {
		ks.value = committedOp.Value
	} else if committedOp.Type == OpDelete {
		ks.value = ""
	}
	committed := ks.value
	ks.mu.Unlock()

	s.metrics.RecordWrite(time.Since(start))
	return committed, nil
}

// Delete durably removes key via a Paxos round.
func (s *Store) Delete(ctx context.Context, key string) error {
	start := time.Now()
	ks := s.keyInstance(key)

	op := Op{Type: OpDelete, Key: key, Nonce: fmt.Sprintf("%d", time.Now().UnixNano())}
	payload, err := encodeOp(op)
	if err != nil {
		return err
	}

	chosen, err := ks.proposer.Propose(ctx, payload)
	if err != nil {
		return err
	}

	committedOp, _ := decodeOp(chosen)
	ks.mu.Lock()
	if committedOp.Type == OpSet {
		ks.value = committedOp.Value
	} else if committedOp.Type == OpDelete {
		ks.value = ""
	}
	ks.mu.Unlock()

	s.metrics.RecordWrite(time.Since(start))
	return nil
}

// Get performs a strong linearisable read by running a Noop Paxos round on
// this key's instance, ensuring the local state reflects any committed value.
func (s *Store) Get(ctx context.Context, key string) (string, bool, error) {
	start := time.Now()
	ks := s.keyInstance(key)

	op := Op{Type: OpNoop, Nonce: fmt.Sprintf("%d", time.Now().UnixNano())}
	payload, err := encodeOp(op)
	if err != nil {
		return "", false, err
	}

	chosen, err := ks.proposer.Propose(ctx, payload)
	if err != nil {
		return "", false, err
	}

	committedOp, _ := decodeOp(chosen)
	ks.mu.Lock()
	if committedOp.Type == OpSet {
		ks.value = committedOp.Value
	} else if committedOp.Type == OpDelete {
		ks.value = ""
	}
	v := ks.value
	ks.mu.Unlock()

	s.metrics.RecordRead(time.Since(start))
	ok := v != ""
	return v, ok, nil
}

// GetLocal returns the locally cached value without running a Paxos round.
// This is NOT linearisable but has zero network cost.
func (s *Store) GetLocal(key string) (string, bool) {
	ks := s.keyInstance(key)
	ks.mu.RLock()
	v := ks.value
	ks.mu.RUnlock()
	return v, v != ""
}

// Snapshot returns a copy of the entire key-value map.
func (s *Store) Snapshot() map[string]string {
	s.mu.Lock()
	keys := make([]string, 0, len(s.perKey))
	for k := range s.perKey {
		keys = append(keys, k)
	}
	s.mu.Unlock()

	out := make(map[string]string, len(keys))
	for _, k := range keys {
		ks := s.keyInstance(k)
		ks.mu.RLock()
		if ks.value != "" {
			out[k] = ks.value
		}
		ks.mu.RUnlock()
	}
	return out
}
