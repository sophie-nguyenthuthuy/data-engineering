// Package simulation implements a multi-region distributed system simulation.
// Time is scaled: 1 second of wall time = 1 minute of simulated time.
// This lets us simulate 5-minute partitions in 5 seconds of real time.
package simulation

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/crdt"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/itc"
)

const TimeScale = 60 // 1 wall second = 60 simulated seconds

// NodeState holds the CRDT state for a node.
// We use a shared counter (PNCounter) and a membership set (ORSet) as the demo workload.
type NodeState struct {
	Counter crdt.PNCounter
	Members crdt.ORSetState[string]
}

// newNodeState returns a zero-initialized node state.
func newNodeState() NodeState {
	return NodeState{
		Counter: crdt.NewPNCounter(),
		Members: crdt.NewORSet[string](),
	}
}

// Message is a state message between nodes (delta sync).
type Message struct {
	From    causal.NodeID
	State   NodeState // full state for simplicity in the simulation
	SentAt  time.Time
	Latency time.Duration
}

// Node represents one replica in the distributed system.
type Node struct {
	mu         sync.RWMutex
	ID         causal.NodeID
	Region     string
	state      NodeState
	inbox      chan Message
	outbox     chan Message
	network    *Network
	itcNode    *itc.AntiEntropyNode
	writeCount atomic.Int64
	mergeCount atomic.Int64
	rng        *rand.Rand
	stopped    atomic.Bool
}

func NewNode(id causal.NodeID, region string, net *Network) *Node {
	return &Node{
		ID:      id,
		Region:  region,
		state:   newNodeState(),
		inbox:   make(chan Message, 256),
		outbox:  make(chan Message, 256),
		network: net,
		itcNode: itc.NewAntiEntropyNode(string(id)),
		rng:     rand.New(rand.NewSource(int64(hashString(string(id))))),
	}
}

// State returns a snapshot of this node's CRDT state.
func (n *Node) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// Increment performs a local counter increment.
func (n *Node) Increment() {
	n.mu.Lock()
	newState, _ := crdt.PNCounterIncrement(n.state.Counter, n.ID)
	n.state.Counter = newState
	n.mu.Unlock()
	n.itcNode.RecordEvent()
	n.writeCount.Add(1)
}

// Decrement performs a local counter decrement.
func (n *Node) Decrement() {
	n.mu.Lock()
	newState, _ := crdt.PNCounterDecrement(n.state.Counter, n.ID)
	n.state.Counter = newState
	n.mu.Unlock()
	n.itcNode.RecordEvent()
	n.writeCount.Add(1)
}

// AddMember adds a member to the ORSet.
func (n *Node) AddMember(member string) {
	n.mu.Lock()
	newState, _ := crdt.ORSetAdd(n.state.Members, member, n.ID)
	n.state.Members = newState
	n.mu.Unlock()
	n.itcNode.RecordEvent()
	n.writeCount.Add(1)
}

// RemoveMember removes a member from the ORSet.
func (n *Node) RemoveMember(member string) {
	n.mu.Lock()
	newState, _ := crdt.ORSetRemove(n.state.Members, member, n.ID)
	n.state.Members = newState
	n.mu.Unlock()
	n.itcNode.RecordEvent()
	n.writeCount.Add(1)
}

// Merge merges an incoming state into this node's state.
func (n *Node) Merge(incoming NodeState) {
	n.mu.Lock()
	n.state.Counter = crdt.PNCounterOps.Join(n.state.Counter, incoming.Counter)
	n.state.Members = crdt.ORSetOps[string]().Join(n.state.Members, incoming.Members)
	n.mu.Unlock()
	n.mergeCount.Add(1)
}

// BroadcastState sends this node's current state to all reachable peers.
func (n *Node) BroadcastState() {
	snapshot := n.State()
	peers := n.network.Peers(n.ID)
	for _, peerID := range peers {
		msg := Message{
			From:    n.ID,
			State:   snapshot,
			SentAt:  time.Now(),
			Latency: n.network.Latency(n.ID, peerID),
		}
		n.network.Send(peerID, msg)
	}
}

// Run starts the node's main loop: periodic writes and gossip.
func (n *Node) Run(stop <-chan struct{}) {
	gossipTick := time.NewTicker(200 * time.Millisecond) // gossip every 200ms wall = ~12s simulated
	writeTick := time.NewTicker(150 * time.Millisecond)  // writes every 150ms
	defer gossipTick.Stop()
	defer writeTick.Stop()

	for {
		select {
		case <-stop:
			n.stopped.Store(true)
			return

		case msg := <-n.inbox:
			// Simulate network latency
			time.Sleep(msg.Latency / TimeScale)
			n.Merge(msg.State)

		case <-gossipTick.C:
			n.BroadcastState()

		case <-writeTick.C:
			// Random workload: mix of increments/decrements and member updates
			switch n.rng.Intn(4) {
			case 0:
				n.Increment()
			case 1:
				n.Decrement()
			case 2:
				member := fmt.Sprintf("user-%d", n.rng.Intn(10))
				n.AddMember(member)
			case 3:
				member := fmt.Sprintf("user-%d", n.rng.Intn(10))
				n.RemoveMember(member)
			}
		}
	}
}

// Stats returns observability metrics for this node.
func (n *Node) Stats() NodeStats {
	return NodeStats{
		ID:           n.ID,
		Region:       n.Region,
		Writes:       n.writeCount.Load(),
		Merges:       n.mergeCount.Load(),
		ITCMetadata:  n.itcNode.MetadataSize(),
		CounterValue: crdt.PNCounterValue(n.State().Counter),
		MemberCount:  len(crdt.ORSetElements(n.State().Members)),
	}
}

// NodeStats is a snapshot of node metrics.
type NodeStats struct {
	ID           causal.NodeID
	Region       string
	Writes       int64
	Merges       int64
	ITCMetadata  int
	CounterValue int64
	MemberCount  int
}

func (s NodeStats) String() string {
	return fmt.Sprintf("[%s/%s] counter=%d members=%d writes=%d merges=%d itc_metadata=%d",
		s.Region, s.ID, s.CounterValue, s.MemberCount, s.Writes, s.Merges, s.ITCMetadata)
}

func hashString(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}
