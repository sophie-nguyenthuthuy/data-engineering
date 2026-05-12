package paxos

import (
	"fmt"
	"strings"
)

// Ballot is a globally ordered proposal number combining a monotonic counter and leader ID.
// Lexicographic ordering: higher Number wins; ties broken by LeaderID.
type Ballot struct {
	Number   uint64
	LeaderID string
}

var ZeroBallot = Ballot{}

func (b Ballot) GreaterThan(o Ballot) bool {
	if b.Number != o.Number {
		return b.Number > o.Number
	}
	return strings.Compare(b.LeaderID, o.LeaderID) > 0
}

func (b Ballot) Equal(o Ballot) bool {
	return b.Number == o.Number && b.LeaderID == o.LeaderID
}

func (b Ballot) GreaterOrEqual(o Ballot) bool {
	return b.Equal(o) || b.GreaterThan(o)
}

func (b Ballot) IsZero() bool { return b.Number == 0 && b.LeaderID == "" }

func (b Ballot) String() string { return fmt.Sprintf("(%d,%s)", b.Number, b.LeaderID) }

// Value is an opaque byte slice stored by the consensus layer.
type Value []byte

func (v Value) Equal(o Value) bool {
	if len(v) != len(o) {
		return false
	}
	for i := range v {
		if v[i] != o[i] {
			return false
		}
	}
	return true
}

// --- Phase 1 (Prepare / Promise) -----------------------------------------------

// PrepareMsg is sent by a proposer to start Phase 1 with ballot b.
type PrepareMsg struct {
	Ballot Ballot
	// ConfigEpoch lets acceptors reject stale-config prepares.
	ConfigEpoch uint64
}

// PromiseMsg is an acceptor's Phase 1 response.
// If the acceptor has already voted, it returns its highest voted ballot/value.
type PromiseMsg struct {
	Ballot      Ballot
	AcceptorID  string
	ConfigEpoch uint64
	// Highest ballot in which this acceptor voted (nil = never voted).
	MaxVBal *Ballot
	MaxVal  Value
	// Rejected = true when the acceptor's maxBal already exceeds b.
	Rejected bool
}

// --- Phase 2 (Accept / Accepted) -----------------------------------------------

// AcceptMsg is sent by a proposer to propose value v in ballot b.
type AcceptMsg struct {
	Ballot      Ballot
	Value       Value
	ConfigEpoch uint64
}

// AcceptedMsg is an acceptor's Phase 2 acknowledgement.
type AcceptedMsg struct {
	Ballot      Ballot
	AcceptorID  string
	ConfigEpoch uint64
	Rejected    bool
}

// --- Quorum configuration -------------------------------------------------------

// QuorumConfig captures a Flexible-Paxos quorum assignment.
//
// Safety invariant (Flexible Paxos theorem):
//
//	∀ Q1 ∈ Phase1Quorums, Q2 ∈ Phase2Quorums : Q1 ∩ Q2 ≠ ∅
//
// For simple threshold quorums this reduces to Q1Size + Q2Size > |Acceptors|.
type QuorumConfig struct {
	Acceptors []string
	Q1Size    int // minimum Phase 1 quorum size
	Q2Size    int // minimum Phase 2 quorum size
	Epoch     uint64
}

// Valid reports whether the quorum configuration satisfies the Flexible Paxos
// intersection property.
func (qc QuorumConfig) Valid() bool {
	n := len(qc.Acceptors)
	return n > 0 && qc.Q1Size > 0 && qc.Q2Size > 0 &&
		qc.Q1Size+qc.Q2Size > n &&
		qc.Q1Size <= n && qc.Q2Size <= n
}

// Classic returns a classic-Paxos majority config.
func Classic(acceptors []string) QuorumConfig {
	n := len(acceptors)
	maj := n/2 + 1
	return QuorumConfig{Acceptors: acceptors, Q1Size: maj, Q2Size: maj}
}

// Chosen records a decided (chosen) value.
type Chosen struct {
	Ballot Ballot
	Value  Value
	Epoch  uint64
}
