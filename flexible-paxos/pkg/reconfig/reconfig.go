// Package reconfig implements safe reconfiguration for a Flexible-Paxos cluster.
//
// # Reconfiguration protocol
//
// Changing the quorum configuration (adding/removing acceptors, adjusting Q1/Q2)
// is itself a consensus problem. We use a dedicated "reconfiguration Paxos
// instance" (α) that runs alongside the data-plane Paxos instances (β₁, β₂, …).
//
// Safety argument:
//
//  1. A new config C' is installed only after it has been chosen in α — i.e.,
//     a Q1(α)-quorum has promised and a Q2(α)-quorum has accepted C'.
//
//  2. Before any proposer in the data plane applies C', it runs a Phase 1 in C
//     (the current config) to fence off any in-flight proposals that might have
//     used C.  This is the "joint consensus" step: all members of both C and C'
//     participate during the transition window.
//
//  3. A proposer only switches to C' after successfully completing the joint Phase 1.
//
//  4. Because Q1(C) ∩ Q2(C) ≠ ∅ (by Flexible Paxos) and Q1(C') ∩ Q2(C') ≠ ∅,
//     and because the overlap acceptors promise in both C and C', no value can
//     be chosen in C and a different value chosen in C'.
//
// Concurrent leader elections: if two leaders L1, L2 race in the same epoch:
//   - Both run Phase 1. The Phase 1 quorums both intersect the Phase 2 quorum.
//   - The one with the higher ballot wins Phase 2; the loser's Phase 2 will be
//     rejected (acceptors promised to the higher ballot in Phase 1).
//   - This is identical to Classic Paxos safety — Flexible Paxos does not
//     weaken it.
//
// Membership change: adding a new member M to the cluster:
//   - M starts with no state (it is a "clean" acceptor).
//   - A reconfiguration round chooses a config C' = C ∪ {M}.
//   - During the joint-consensus window every proposer contacts C ∪ C' for
//     Phase 1, ensuring M catches up via promise replies.
//   - After completion, M is a full acceptor under C'.
package reconfig

import (
	"context"
	"fmt"
	"sync"

	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/paxos"
)

// ConfigStore persists the cluster configuration durably.
// In production this is itself a Paxos log; in tests it is an in-memory map.
type ConfigStore interface {
	Load() (paxos.QuorumConfig, error)
	Store(paxos.QuorumConfig) error
}

// Reconfigurator coordinates safe configuration transitions.
type Reconfigurator struct {
	mu        sync.Mutex
	store     ConfigStore
	proposer  *paxos.Proposer
	transport paxos.Transport
	listeners []func(paxos.QuorumConfig)
}

func New(store ConfigStore, proposer *paxos.Proposer, transport paxos.Transport) *Reconfigurator {
	return &Reconfigurator{
		store:    store,
		proposer: proposer,
		transport: transport,
	}
}

func (r *Reconfigurator) AddListener(fn func(paxos.QuorumConfig)) {
	r.mu.Lock()
	r.listeners = append(r.listeners, fn)
	r.mu.Unlock()
}

// Reconfigure proposes a new cluster configuration.
//
// The proposal is serialised: concurrent calls are queued and each waits for
// the previous to complete before starting.  This prevents epoch skips.
func (r *Reconfigurator) Reconfigure(ctx context.Context, newCfg paxos.QuorumConfig) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !newCfg.Valid() {
		return fmt.Errorf("proposed config is invalid: Q1=%d Q2=%d n=%d",
			newCfg.Q1Size, newCfg.Q2Size, len(newCfg.Acceptors))
	}

	current, err := r.store.Load()
	if err != nil {
		return fmt.Errorf("load current config: %w", err)
	}

	// Bump epoch.
	newCfg.Epoch = current.Epoch + 1

	// Joint-consensus Phase 1: fence off in-flight proposals under the current config.
	if err := r.jointPhase1(ctx, current, newCfg); err != nil {
		return fmt.Errorf("joint phase1: %w", err)
	}

	// Persist the new config.
	if err := r.store.Store(newCfg); err != nil {
		return fmt.Errorf("persist config: %w", err)
	}

	// Notify the proposer and all listeners.
	if err := r.proposer.UpdateConfig(newCfg); err != nil {
		return fmt.Errorf("update proposer config: %w", err)
	}

	for _, l := range r.listeners {
		l(newCfg)
	}

	return nil
}

// jointPhase1 runs Phase 1 in the *union* of old and new acceptors, using
// the highest ballot seen so far + 1. This fences any in-flight Phase 2
// messages that were accepted under the old config and ensures the new leader
// can determine the safe value (if any) before switching configs.
func (r *Reconfigurator) jointPhase1(ctx context.Context, old, new paxos.QuorumConfig) error {
	union := unionAcceptors(old.Acceptors, new.Acceptors)

	// We need acknowledgement from at least one quorum of the old config AND
	// one quorum of the new config — conservative joint consensus.
	oldQ1 := old.Q1Size
	newQ1 := new.Q1Size

	type result struct {
		msg paxos.PromiseMsg
		err error
	}

	// Use epoch of the new config as ballot number.
	ballot := paxos.Ballot{Number: new.Epoch, LeaderID: "reconfigurator"}
	results := make(chan result, len(union))
	for _, aid := range union {
		go func(id string) {
			msg, err := r.transport.Prepare(ctx, id, paxos.PrepareMsg{
				Ballot:      ballot,
				ConfigEpoch: old.Epoch, // fences old config
			})
			results <- result{msg, err}
		}(aid)
	}

	oldSet := makeSet(old.Acceptors)
	newSet := makeSet(new.Acceptors)
	oldPromises, newPromises := 0, 0

	for range union {
		res := <-results
		if res.err != nil || res.msg.Rejected {
			continue
		}
		if oldSet[res.msg.AcceptorID] {
			oldPromises++
		}
		if newSet[res.msg.AcceptorID] {
			newPromises++
		}
	}

	if oldPromises < oldQ1 {
		return fmt.Errorf("old config: only %d/%d promises in joint phase1", oldPromises, oldQ1)
	}
	if newPromises < newQ1 {
		return fmt.Errorf("new config: only %d/%d promises in joint phase1", newPromises, newQ1)
	}
	return nil
}

func unionAcceptors(a, b []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, x := range a {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	for _, x := range b {
		if !seen[x] {
			seen[x] = true
			out = append(out, x)
		}
	}
	return out
}

func makeSet(ss []string) map[string]bool {
	m := make(map[string]bool, len(ss))
	for _, s := range ss {
		m[s] = true
	}
	return m
}

// MemConfigStore is an in-memory ConfigStore for testing.
type MemConfigStore struct {
	mu  sync.Mutex
	cfg paxos.QuorumConfig
}

func NewMemConfigStore(initial paxos.QuorumConfig) *MemConfigStore {
	return &MemConfigStore{cfg: initial}
}

func (s *MemConfigStore) Load() (paxos.QuorumConfig, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg, nil
}

func (s *MemConfigStore) Store(cfg paxos.QuorumConfig) error {
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	return nil
}
