package itc

import (
	"fmt"
	"sync"
	"time"
)

// AntiEntropyNode wraps a node's ITC stamp for anti-entropy synchronization.
//
// Anti-entropy with ITC works as follows:
//  1. Each node maintains a Stamp (ID, Event).
//  2. Periodically, nodes exchange stamps with random peers.
//  3. If peer.Event ≤ local.Event, local has nothing to learn from peer.
//  4. If local.Event ≤ peer.Event, local needs to sync from peer.
//  5. If concurrent, both need to exchange data.
//
// Bounded metadata: ITC stamps have size O(k) where k = number of active forks.
// This is in contrast to vector clocks which are O(n) for n ever-seen nodes.
type AntiEntropyNode struct {
	mu       sync.RWMutex
	NodeID   string
	stamp    Stamp
	peers    map[string]*AntiEntropyNode
	syncLog  []SyncEvent
	maxLog   int
}

// SyncEvent records one anti-entropy exchange for observability.
type SyncEvent struct {
	At          time.Time
	WithPeer    string
	Direction   string // "sent", "received", "concurrent"
	MetadataSz  int
}

// NewAntiEntropyNode creates a node with an initial seed stamp.
func NewAntiEntropyNode(nodeID string) *AntiEntropyNode {
	return &AntiEntropyNode{
		NodeID: nodeID,
		stamp:  Seed(),
		peers:  make(map[string]*AntiEntropyNode),
		maxLog: 1000,
	}
}

// ForkChild creates a new child node by forking this node's stamp.
// Returns the child node with the forked stamp.
func (n *AntiEntropyNode) ForkChild(childID string) *AntiEntropyNode {
	n.mu.Lock()
	defer n.mu.Unlock()

	parent, child := Fork(n.stamp)
	n.stamp = parent

	childNode := &AntiEntropyNode{
		NodeID: childID,
		stamp:  child,
		peers:  make(map[string]*AntiEntropyNode),
		maxLog: 1000,
	}
	return childNode
}

// RecordEvent marks that this node performed a write/mutation.
func (n *AntiEntropyNode) RecordEvent() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stamp = RecordEvent(n.stamp)
}

// AddPeer registers a peer for anti-entropy.
func (n *AntiEntropyNode) AddPeer(peer *AntiEntropyNode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[peer.NodeID] = peer
}

// Stamp returns the current stamp (read-only copy).
func (n *AntiEntropyNode) GetStamp() Stamp {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return Stamp{ID: n.stamp.ID.Clone(), Event: n.stamp.Event.Clone()}
}

// SyncWith performs an anti-entropy exchange with a peer.
// Returns the relationship: "dominated", "dominates", "concurrent", "equal"
func (n *AntiEntropyNode) SyncWith(peer *AntiEntropyNode) string {
	myStamp := n.GetStamp()
	peerStamp := peer.GetStamp()

	var relation string
	switch {
	case LeqEvent(myStamp.Event, peerStamp.Event) && LeqEvent(peerStamp.Event, myStamp.Event):
		relation = "equal"
	case LeqEvent(myStamp.Event, peerStamp.Event):
		// Peer is ahead: absorb peer's knowledge
		n.mu.Lock()
		n.stamp = Join(n.stamp, peerStamp)
		n.mu.Unlock()
		relation = "dominated"
	case LeqEvent(peerStamp.Event, myStamp.Event):
		// We're ahead: peer should absorb our knowledge (they'll do it on their sync round)
		relation = "dominates"
	default:
		// Concurrent: both learn from each other
		n.mu.Lock()
		n.stamp = Join(n.stamp, peerStamp)
		n.mu.Unlock()

		peer.mu.Lock()
		peer.stamp = Join(peer.stamp, myStamp)
		peer.mu.Unlock()
		relation = "concurrent"
	}

	sz := MetadataSize(n.GetStamp())
	n.mu.Lock()
	n.syncLog = append(n.syncLog, SyncEvent{
		At: time.Now(), WithPeer: peer.NodeID, Direction: relation, MetadataSz: sz,
	})
	if len(n.syncLog) > n.maxLog {
		n.syncLog = n.syncLog[len(n.syncLog)-n.maxLog:]
	}
	n.mu.Unlock()

	return relation
}

// MetadataSize returns the current stamp's metadata cost.
func (n *AntiEntropyNode) MetadataSize() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return MetadataSize(n.stamp)
}

// MissingFrom returns true if this node has events that other doesn't.
func (n *AntiEntropyNode) MissingFrom(other *AntiEntropyNode) bool {
	myStamp := n.GetStamp()
	otherStamp := other.GetStamp()
	return !LeqEvent(myStamp.Event, otherStamp.Event)
}

// AntiEntropyReport summarizes metadata bounds across a cluster.
type AntiEntropyReport struct {
	Nodes         []string
	MaxMetadata   int
	MinMetadata   int
	TotalSyncs    int
	Convergence   bool
}

// ClusterReport generates a metadata-size report for a set of nodes.
func ClusterReport(nodes []*AntiEntropyNode) AntiEntropyReport {
	report := AntiEntropyReport{
		MinMetadata: 1<<31 - 1,
		Convergence: true,
	}

	stamps := make([]Stamp, len(nodes))
	for i, n := range nodes {
		stamps[i] = n.GetStamp()
		report.Nodes = append(report.Nodes, n.NodeID)
		sz := n.MetadataSize()
		if sz > report.MaxMetadata {
			report.MaxMetadata = sz
		}
		if sz < report.MinMetadata {
			report.MinMetadata = sz
		}
		report.TotalSyncs += len(n.syncLog)
	}

	// Check convergence: all nodes should have equivalent event trees
	for i := 1; i < len(stamps); i++ {
		if !LeqEvent(stamps[0].Event, stamps[i].Event) || !LeqEvent(stamps[i].Event, stamps[0].Event) {
			report.Convergence = false
			break
		}
	}

	return report
}

func (r AntiEntropyReport) String() string {
	return fmt.Sprintf(
		"nodes=%d metadata=[%d..%d] syncs=%d converged=%v",
		len(r.Nodes), r.MinMetadata, r.MaxMetadata, r.TotalSyncs, r.Convergence,
	)
}
