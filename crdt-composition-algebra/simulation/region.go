package simulation

import (
	"fmt"
	"time"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/causal"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/crdt"
)

// RegionConfig defines latency parameters for a multi-region topology.
type RegionConfig struct {
	Name              string
	NodesPerRegion    int
	IntraRegionDelay  time.Duration // simulated latency within region
	InterRegionDelay  time.Duration // simulated latency cross-region
}

// MultiRegionCluster sets up a cluster with multiple geographic regions.
// Within a region: low latency (1-5ms). Cross-region: 50-150ms.
type MultiRegionCluster struct {
	Regions map[string][]*Node
	Network *Network
	All     []*Node
}

// NewMultiRegionCluster creates a multi-region cluster.
func NewMultiRegionCluster(configs []RegionConfig) *MultiRegionCluster {
	net := NewNetwork()
	c := &MultiRegionCluster{
		Regions: make(map[string][]*Node),
		Network: net,
	}

	// Create nodes
	for _, cfg := range configs {
		for i := 0; i < cfg.NodesPerRegion; i++ {
			id := causal.NodeID(fmt.Sprintf("%s-node-%d", cfg.Name, i))
			node := NewNode(id, cfg.Name, net)
			c.Regions[cfg.Name] = append(c.Regions[cfg.Name], node)
			c.All = append(c.All, node)
			net.Register(node)
		}
	}

	// Configure latencies
	for _, cfgA := range configs {
		nodesA := c.Regions[cfgA.Name]
		for _, cfgB := range configs {
			nodesB := c.Regions[cfgB.Name]
			for _, a := range nodesA {
				for _, b := range nodesB {
					if a.ID == b.ID {
						continue
					}
					var delay time.Duration
					if cfgA.Name == cfgB.Name {
						delay = cfgA.IntraRegionDelay
					} else {
						delay = cfgA.InterRegionDelay
					}
					net.SetLatency(a.ID, b.ID, delay)
				}
			}
		}
	}

	return c
}

// Start launches all nodes.
func (c *MultiRegionCluster) Start() (stop func()) {
	stopCh := make(chan struct{})
	for _, node := range c.All {
		go node.Run(stopCh)
	}
	return func() { close(stopCh) }
}

// PartitionRegion isolates all nodes in a region from all other regions.
// Intra-region communication continues; inter-region is blocked.
func (c *MultiRegionCluster) PartitionRegion(region string) []causal.NodeID {
	nodes := c.Regions[region]
	ids := make([]causal.NodeID, len(nodes))
	for i, n := range nodes {
		ids[i] = n.ID
	}
	c.Network.Partition(ids)
	return ids
}

// HealAll removes all partitions.
func (c *MultiRegionCluster) HealAll() {
	c.Network.HealPartitions()
}

// Stats returns a snapshot of all node stats.
func (c *MultiRegionCluster) Stats() []NodeStats {
	stats := make([]NodeStats, len(c.All))
	for i, n := range c.All {
		stats[i] = n.Stats()
	}
	return stats
}

// WaitForConvergence polls until all nodes agree or timeout.
// Returns true if converged, false if timed out.
func (c *MultiRegionCluster) WaitForConvergence(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.IsConverged() {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// IsConverged checks if all nodes have the same CRDT state.
func (c *MultiRegionCluster) IsConverged() bool {
	if len(c.All) == 0 {
		return true
	}
	ref := c.All[0].State()
	refVal := crdt.PNCounterValue(ref.Counter)
	refMembers := memberSetFrom(crdt.ORSetElements(ref.Members))

	for _, n := range c.All[1:] {
		s := n.State()
		if crdt.PNCounterValue(s.Counter) != refVal {
			return false
		}
		if !setsEq(refMembers, memberSetFrom(crdt.ORSetElements(s.Members))) {
			return false
		}
	}
	return true
}

func memberSetFrom(elems []string) map[string]bool {
	m := make(map[string]bool, len(elems))
	for _, e := range elems {
		m[e] = true
	}
	return m
}

func setsEq(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}
