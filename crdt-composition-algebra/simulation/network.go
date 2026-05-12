package simulation

import (
	"fmt"
	"sync"
	"time"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
)

// PartitionEvent records a network partition for observability.
type PartitionEvent struct {
	StartAt  time.Time
	HealAt   time.Time
	Isolated []causal.NodeID
	Duration time.Duration
}

// Network simulates the message-passing layer between nodes.
// It supports latency, partitions, and message loss.
type Network struct {
	mu         sync.RWMutex
	nodes      map[causal.NodeID]*Node
	latencies  map[string]time.Duration // key: "from->to"
	partitions map[causal.NodeID]map[causal.NodeID]bool // partitioned[a][b] = true means a cannot reach b
	history    []PartitionEvent
	dropped    int64
	delivered  int64
}

func NewNetwork() *Network {
	return &Network{
		nodes:      make(map[causal.NodeID]*Node),
		latencies:  make(map[string]time.Duration),
		partitions: make(map[causal.NodeID]map[causal.NodeID]bool),
	}
}

// Register adds a node to the network.
func (net *Network) Register(n *Node) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.nodes[n.ID] = n
}

// SetLatency configures one-way latency between two nodes (in simulated time).
func (net *Network) SetLatency(from, to causal.NodeID, d time.Duration) {
	net.mu.Lock()
	defer net.mu.Unlock()
	key := fmt.Sprintf("%s->%s", from, to)
	net.latencies[key] = d
}

// Latency returns the configured latency between two nodes.
func (net *Network) Latency(from, to causal.NodeID) time.Duration {
	net.mu.RLock()
	defer net.mu.RUnlock()
	key := fmt.Sprintf("%s->%s", from, to)
	if d, ok := net.latencies[key]; ok {
		return d
	}
	return 10 * time.Millisecond
}

// Partition creates a network partition that isolates a group of nodes.
// Nodes in isolated cannot send to or receive from nodes outside.
func (net *Network) Partition(isolated []causal.NodeID) {
	net.mu.Lock()
	defer net.mu.Unlock()

	isolatedSet := make(map[causal.NodeID]bool, len(isolated))
	for _, id := range isolated {
		isolatedSet[id] = true
	}

	for _, a := range isolated {
		if net.partitions[a] == nil {
			net.partitions[a] = make(map[causal.NodeID]bool)
		}
		for id := range net.nodes {
			if !isolatedSet[id] {
				net.partitions[a][id] = true
				// Also block the reverse direction
				if net.partitions[id] == nil {
					net.partitions[id] = make(map[causal.NodeID]bool)
				}
				net.partitions[id][a] = true
			}
		}
	}

	net.history = append(net.history, PartitionEvent{
		StartAt:  time.Now(),
		Isolated: isolated,
	})
}

// HealPartitions removes all active partitions.
func (net *Network) HealPartitions() {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.partitions = make(map[causal.NodeID]map[causal.NodeID]bool)

	now := time.Now()
	for i := range net.history {
		if net.history[i].HealAt.IsZero() {
			net.history[i].HealAt = now
			net.history[i].Duration = now.Sub(net.history[i].StartAt)
		}
	}
}

// IsPartitioned returns true if from cannot reach to.
func (net *Network) IsPartitioned(from, to causal.NodeID) bool {
	net.mu.RLock()
	defer net.mu.RUnlock()
	if pm, ok := net.partitions[from]; ok {
		return pm[to]
	}
	return false
}

// Send delivers a message to the target node's inbox (if not partitioned).
func (net *Network) Send(to causal.NodeID, msg Message) {
	net.mu.RLock()
	partitioned := false
	if pm, ok := net.partitions[msg.From]; ok && pm[to] {
		partitioned = true
	}
	node := net.nodes[to]
	net.mu.RUnlock()

	if partitioned || node == nil {
		net.mu.Lock()
		net.dropped++
		net.mu.Unlock()
		return
	}

	select {
	case node.inbox <- msg:
		net.mu.Lock()
		net.delivered++
		net.mu.Unlock()
	default:
		// Drop if inbox full (simulates congestion)
		net.mu.Lock()
		net.dropped++
		net.mu.Unlock()
	}
}

// Peers returns all nodes reachable from the given node.
func (net *Network) Peers(from causal.NodeID) []causal.NodeID {
	net.mu.RLock()
	defer net.mu.RUnlock()
	peers := make([]causal.NodeID, 0, len(net.nodes))
	for id := range net.nodes {
		if id == from {
			continue
		}
		if pm, ok := net.partitions[from]; ok && pm[id] {
			continue
		}
		peers = append(peers, id)
	}
	return peers
}

// Stats returns network-level metrics.
func (net *Network) Stats() NetworkStats {
	net.mu.RLock()
	defer net.mu.RUnlock()
	return NetworkStats{
		Delivered:  net.delivered,
		Dropped:    net.dropped,
		Partitions: len(net.history),
	}
}

// PartitionHistory returns a copy of the partition history.
func (net *Network) PartitionHistory() []PartitionEvent {
	net.mu.RLock()
	defer net.mu.RUnlock()
	result := make([]PartitionEvent, len(net.history))
	copy(result, net.history)
	return result
}

// NetworkStats is a summary of network metrics.
type NetworkStats struct {
	Delivered  int64
	Dropped    int64
	Partitions int
}
