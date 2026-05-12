package paxos

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

// Transport abstracts the RPC layer so proposers can work with both
// in-memory acceptors (tests) and remote ones (production).
type Transport interface {
	Prepare(ctx context.Context, acceptorID string, msg PrepareMsg) (PromiseMsg, error)
	Accept(ctx context.Context, acceptorID string, msg AcceptMsg) (AcceptedMsg, error)
}

// Proposer drives the two-phase Flexible-Paxos protocol.
//
// It tracks the highest ballot it has ever used so it can always propose a
// strictly larger one. Phase 1 only needs a Q1-quorum; Phase 2 only needs a
// Q2-quorum. Safety is guaranteed as long as Q1 ∩ Q2 ≠ ∅ (enforced by
// QuorumConfig.Valid).
type Proposer struct {
	id        string
	transport Transport

	mu          sync.Mutex
	config      QuorumConfig
	highBallot  atomic.Uint64 // monotonic counter shared across proposals
}

func NewProposer(id string, config QuorumConfig, transport Transport) (*Proposer, error) {
	if !config.Valid() {
		return nil, fmt.Errorf("invalid quorum config: Q1=%d Q2=%d n=%d",
			config.Q1Size, config.Q2Size, len(config.Acceptors))
	}
	return &Proposer{id: id, config: config, transport: transport}, nil
}

// UpdateConfig installs a new quorum configuration (called by the reconfigurator).
func (p *Proposer) UpdateConfig(cfg QuorumConfig) error {
	if !cfg.Valid() {
		return fmt.Errorf("invalid quorum config")
	}
	p.mu.Lock()
	p.config = cfg
	p.mu.Unlock()
	return nil
}

func (p *Proposer) Config() QuorumConfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.config
}

// nextBallot allocates the next ballot number, strictly greater than any
// previously allocated.
func (p *Proposer) nextBallot() Ballot {
	return Ballot{Number: p.highBallot.Add(1), LeaderID: p.id}
}

// Propose runs a full Paxos round (Phase 1 + Phase 2) and returns the
// chosen value.  If a prior value was already chosen for this slot, the
// returned value may differ from the requested one (classic Paxos safety).
//
// Propose is concurrency-safe: multiple goroutines may call it simultaneously
// (they will contend on ballot numbers).
func (p *Proposer) Propose(ctx context.Context, proposed Value) (Value, error) {
	for {
		// Check for context cancellation before each retry.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		ballot := p.nextBallot()

		// --- Phase 1 -------------------------------------------------------
		// Phase 1 failures (insufficient promises) mean a higher ballot is
		// already active. Retry with a fresh ballot rather than returning an
		// error — competing leaders must eventually converge.
		safeVal, err := p.phase1(ctx, ballot)
		if err != nil {
			continue
		}

		// Choose the value: if the quorum returned a previously voted value,
		// we MUST propose that (Paxos safety). Otherwise propose ours.
		value := proposed
		if safeVal != nil {
			value = safeVal
		}

		// --- Phase 2 -------------------------------------------------------
		if err := p.phase2(ctx, ballot, value); err != nil {
			// A rejection in Phase 2 means a higher ballot exists; retry.
			continue
		}

		return value, nil
	}
}

// phase1 sends Prepare to a Q1-quorum. Returns the safe value (highest-balloted
// vote from the quorum) or nil if no quorum member has voted.
func (p *Proposer) phase1(ctx context.Context, ballot Ballot) (Value, error) {
	p.mu.Lock()
	cfg := p.config
	p.mu.Unlock()

	type result struct {
		msg PromiseMsg
		err error
	}

	results := make(chan result, len(cfg.Acceptors))
	for _, aid := range cfg.Acceptors {
		go func(id string) {
			msg, err := p.transport.Prepare(ctx, id, PrepareMsg{
				Ballot:      ballot,
				ConfigEpoch: cfg.Epoch,
			})
			results <- result{msg, err}
		}(aid)
	}

	var promises []PromiseMsg
	rejected := 0
	for range cfg.Acceptors {
		r := <-results
		if r.err != nil || r.msg.Rejected {
			rejected++
			continue
		}
		promises = append(promises, r.msg)
		if len(promises) >= cfg.Q1Size {
			// Q1-quorum reached; stop waiting (remaining goroutines drain naturally).
			break
		}
	}

	if len(promises) < cfg.Q1Size {
		return nil, fmt.Errorf("phase1 failed: only %d/%d promises (ballot %v)",
			len(promises), cfg.Q1Size, ballot)
	}

	// Pick the value associated with the highest voted ballot.
	return highestVoted(promises), nil
}

// highestVoted returns the value from the promise with the highest maxVBal,
// or nil if no promise contains a prior vote.
func highestVoted(promises []PromiseMsg) Value {
	var best *Ballot
	var bestVal Value
	for _, p := range promises {
		if p.MaxVBal == nil {
			continue
		}
		if best == nil || p.MaxVBal.GreaterThan(*best) {
			b := *p.MaxVBal
			best = &b
			bestVal = p.MaxVal
		}
	}
	return bestVal
}

// phase2 sends Accept to a Q2-quorum. Returns nil on success.
func (p *Proposer) phase2(ctx context.Context, ballot Ballot, value Value) error {
	p.mu.Lock()
	cfg := p.config
	p.mu.Unlock()

	type result struct {
		msg AcceptedMsg
		err error
	}

	results := make(chan result, len(cfg.Acceptors))
	for _, aid := range cfg.Acceptors {
		go func(id string) {
			msg, err := p.transport.Accept(ctx, id, AcceptMsg{
				Ballot:      ballot,
				Value:       value,
				ConfigEpoch: cfg.Epoch,
			})
			results <- result{msg, err}
		}(aid)
	}

	accepted := 0
	for range cfg.Acceptors {
		r := <-results
		if r.err != nil || r.msg.Rejected {
			continue
		}
		accepted++
		if accepted >= cfg.Q2Size {
			return nil
		}
	}

	return fmt.Errorf("phase2 failed: only %d/%d accepted (ballot %v)",
		accepted, cfg.Q2Size, ballot)
}
