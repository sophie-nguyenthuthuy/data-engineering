package paxos

import (
	"context"
	"fmt"
	"sync"
)

// Acceptor is a Flexible-Paxos acceptor node.
//
// State variables per the Paxos literature:
//   - maxBal  : highest ballot seen in Phase 1 (promised not to accept lower)
//   - maxVBal : highest ballot in which this acceptor voted (Phase 2)
//   - maxVal  : value voted in maxVBal
//
// Acceptors are passive: they only respond to messages from proposers.
// All state mutations are serialised through a mutex so the acceptor is safe
// for concurrent RPCs.
type Acceptor struct {
	mu          sync.Mutex
	id          string
	configEpoch uint64

	// Durable Paxos state (must survive crashes in a real system).
	maxBal  Ballot
	maxVBal *Ballot
	maxVal  Value
}

func NewAcceptor(id string) *Acceptor {
	return &Acceptor{id: id}
}

func (a *Acceptor) ID() string { return a.id }

// UpdateConfig updates the epoch visible to this acceptor.
// Prepares/accepts with a stale epoch are rejected.
func (a *Acceptor) UpdateConfig(epoch uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if epoch > a.configEpoch {
		a.configEpoch = epoch
	}
}

// Prepare handles a Phase 1 Prepare message.
//
// An acceptor promises to ignore any ballot < b iff b > maxBal.
// It returns its highest voted ballot + value so the proposer can
// reconstruct any previously chosen value.
func (a *Acceptor) Prepare(_ context.Context, msg PrepareMsg) (PromiseMsg, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if msg.ConfigEpoch < a.configEpoch {
		return PromiseMsg{}, fmt.Errorf("stale config epoch %d (current %d)",
			msg.ConfigEpoch, a.configEpoch)
	}

	resp := PromiseMsg{
		Ballot:      msg.Ballot,
		AcceptorID:  a.id,
		ConfigEpoch: a.configEpoch,
		MaxVBal:     a.maxVBal,
		MaxVal:      a.maxVal,
	}

	if msg.Ballot.GreaterThan(a.maxBal) {
		a.maxBal = msg.Ballot
		resp.Rejected = false
	} else {
		resp.Rejected = true
	}
	return resp, nil
}

// Accept handles a Phase 2 Accept message.
//
// The acceptor votes for (b, v) iff b >= maxBal (it has not promised to
// ignore b in Phase 1).
func (a *Acceptor) Accept(_ context.Context, msg AcceptMsg) (AcceptedMsg, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if msg.ConfigEpoch < a.configEpoch {
		return AcceptedMsg{}, fmt.Errorf("stale config epoch %d (current %d)",
			msg.ConfigEpoch, a.configEpoch)
	}

	resp := AcceptedMsg{
		Ballot:      msg.Ballot,
		AcceptorID:  a.id,
		ConfigEpoch: a.configEpoch,
	}

	if msg.Ballot.GreaterOrEqual(a.maxBal) {
		a.maxBal = msg.Ballot
		a.maxVBal = &msg.Ballot
		a.maxVal = msg.Value
		resp.Rejected = false
	} else {
		resp.Rejected = true
	}
	return resp, nil
}

// State returns a snapshot of this acceptor's durable state (for testing/debugging).
func (a *Acceptor) State() (maxBal Ballot, maxVBal *Ballot, maxVal Value) {
	a.mu.Lock()
	defer a.mu.Unlock()
	b := a.maxVBal
	return a.maxBal, b, a.maxVal
}
