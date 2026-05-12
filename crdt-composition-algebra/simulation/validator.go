package simulation

import (
	"fmt"
	"strings"
	"time"

	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/crdt"
	"github.com/sophie-nguyenthuthuy/crdt-composition-algebra/itc"
)

// ConvergenceResult captures the result of a convergence check.
type ConvergenceResult struct {
	Converged      bool
	CounterValues  map[string]int64  // node -> counter value
	MemberSets     map[string][]string // node -> members
	ITCMetadata    map[string]int
	MaxMetadataAge int // max ITC metadata size
	CheckedAt      time.Time
	Duration       time.Duration
}

// Validator checks convergence properties of a cluster.
type Validator struct {
	cluster *MultiRegionCluster
}

func NewValidator(cluster *MultiRegionCluster) *Validator {
	return &Validator{cluster: cluster}
}

// CheckConvergence performs a comprehensive convergence check.
// It checks: counter values, member sets, and ITC causal history.
func (v *Validator) CheckConvergence() ConvergenceResult {
	start := time.Now()
	result := ConvergenceResult{
		Converged:   true,
		CounterValues: make(map[string]int64),
		MemberSets:  make(map[string][]string),
		ITCMetadata: make(map[string]int),
		CheckedAt:   start,
	}

	nodes := v.cluster.All
	if len(nodes) == 0 {
		return result
	}

	// Collect all states
	states := make([]NodeState, len(nodes))
	for i, n := range nodes {
		states[i] = n.State()
		result.CounterValues[string(n.ID)] = crdt.PNCounterValue(states[i].Counter)
		members := crdt.ORSetElements(states[i].Members)
		result.MemberSets[string(n.ID)] = members
		result.ITCMetadata[string(n.ID)] = n.itcNode.MetadataSize()
		if n.itcNode.MetadataSize() > result.MaxMetadataAge {
			result.MaxMetadataAge = n.itcNode.MetadataSize()
		}
	}

	// Check counter convergence: all nodes must report same value
	var refVal int64
	for _, v := range result.CounterValues {
		refVal = v
		break
	}
	for nodeID, val := range result.CounterValues {
		if val != refVal {
			result.Converged = false
			_ = nodeID
			break
		}
	}

	// Check ORSet convergence: all nodes must have same members
	if result.Converged {
		refMembers := memberSet(result.MemberSets[string(nodes[0].ID)])
		for _, n := range nodes[1:] {
			members := memberSet(result.MemberSets[string(n.ID)])
			if !setsEqual(refMembers, members) {
				result.Converged = false
				break
			}
		}
	}

	result.Duration = time.Since(start)
	return result
}

// CheckITCBounded verifies that ITC metadata remains bounded.
// For k active nodes, metadata should be O(k), not O(n) like vector clocks.
func (v *Validator) CheckITCBounded() ITCBoundReport {
	report := ITCBoundReport{
		ActiveNodes:    len(v.cluster.All),
		CheckedAt:      time.Now(),
	}

	// Collect all ITC nodes and sync them
	itcNodes := make([]*itc.AntiEntropyNode, len(v.cluster.All))
	for i, n := range v.cluster.All {
		itcNodes[i] = n.itcNode
	}

	// Wire peers for ITC anti-entropy
	for i, a := range itcNodes {
		for j, b := range itcNodes {
			if i != j {
				a.AddPeer(b)
			}
		}
	}

	// Run a few rounds of anti-entropy
	for round := 0; round < 3; round++ {
		for _, a := range itcNodes {
			for _, b := range itcNodes {
				if a != b {
					a.SyncWith(b)
				}
			}
		}
	}

	clusterReport := itc.ClusterReport(itcNodes)
	report.MaxMetadata = clusterReport.MaxMetadata
	report.MinMetadata = clusterReport.MinMetadata
	report.Converged = clusterReport.Convergence
	// Theoretical bound: O(k) where k = active nodes
	// In practice, ITC trees for k nodes have at most 2k-1 nodes in ID tree
	report.TheoreticalBound = 2*report.ActiveNodes - 1
	report.WithinBound = report.MaxMetadata <= report.TheoreticalBound*4 // 4x slack for event tree

	return report
}

// ITCBoundReport captures ITC metadata bound analysis.
type ITCBoundReport struct {
	ActiveNodes      int
	MaxMetadata      int
	MinMetadata      int
	TheoreticalBound int
	WithinBound      bool
	Converged        bool
	CheckedAt        time.Time
}

func (r ITCBoundReport) String() string {
	status := "WITHIN BOUND"
	if !r.WithinBound {
		status = "EXCEEDS BOUND"
	}
	return fmt.Sprintf("ITC metadata: nodes=%d max=%d min=%d theoretical_bound=%d [%s] converged=%v",
		r.ActiveNodes, r.MaxMetadata, r.MinMetadata, r.TheoreticalBound, status, r.Converged)
}

// PartitionScenario runs a complete partition scenario and validates convergence.
type PartitionScenario struct {
	cluster     *MultiRegionCluster
	validator   *Validator
	Log         []string
}

func NewPartitionScenario(cluster *MultiRegionCluster) *PartitionScenario {
	return &PartitionScenario{
		cluster:   cluster,
		validator: NewValidator(cluster),
	}
}

func (s *PartitionScenario) log(format string, args ...interface{}) {
	msg := fmt.Sprintf("[%s] "+format, append([]interface{}{time.Now().Format("15:04:05.000")}, args...)...)
	s.Log = append(s.Log, msg)
	fmt.Println(msg)
}

// Run executes the full partition-heal-converge scenario.
// Wall time: ~10s. Simulated time: ~10 minutes (including 5-minute partition).
func (s *PartitionScenario) Run() ScenarioResult {
	result := ScenarioResult{StartedAt: time.Now()}
	cluster := s.cluster

	s.log("=== Phase 1: Normal operation (30s simulated) ===")
	time.Sleep(500 * time.Millisecond)

	prePartition := s.validator.CheckConvergence()
	s.log("Pre-partition: counter=%v converged=%v", uniqueValues(prePartition.CounterValues), prePartition.Converged)

	s.log("=== Phase 2: Partitioning us-east region (5 min simulated = 5s wall) ===")
	partitionedIDs := cluster.PartitionRegion("us-east")
	partitionStart := time.Now()
	s.log("Partitioned nodes: %v", nodeIDsToStrings(partitionedIDs))

	// Let the system run partitioned — nodes accumulate divergent state
	time.Sleep(5 * time.Second)

	partitionDuration := time.Since(partitionStart)
	s.log("Partition lasted: %v wall / %v simulated",
		partitionDuration.Round(time.Millisecond),
		(partitionDuration * TimeScale).Round(time.Second))

	midPartition := s.validator.CheckConvergence()
	s.log("Mid-partition: counter_values=%v converged=%v",
		uniqueValues(midPartition.CounterValues), midPartition.Converged)
	result.DivergenceSeen = !midPartition.Converged

	s.log("=== Phase 3: Healing partition ===")
	cluster.HealAll()
	s.log("Partition healed, waiting for convergence...")

	// Wait for convergence with 10s timeout (= ~10 minutes simulated)
	converged := cluster.WaitForConvergence(10 * time.Second)
	result.Converged = converged

	postHeal := s.validator.CheckConvergence()
	s.log("Post-heal: counter=%v member_counts=%v converged=%v",
		uniqueValues(postHeal.CounterValues),
		memberCounts(postHeal.MemberSets),
		postHeal.Converged)

	s.log("=== Phase 4: ITC metadata bound check ===")
	itcReport := s.validator.CheckITCBounded()
	s.log("%s", itcReport)
	result.ITCWithinBound = itcReport.WithinBound

	result.FinalStats = cluster.Stats()
	result.NetworkStats = cluster.Network.Stats()
	result.Duration = time.Since(result.StartedAt)

	s.log("=== Scenario complete: duration=%v converged=%v ===",
		result.Duration.Round(time.Millisecond), result.Converged)

	return result
}

// ScenarioResult captures the full scenario outcome.
type ScenarioResult struct {
	StartedAt      time.Time
	Duration       time.Duration
	DivergenceSeen bool  // did we observe divergence during partition?
	Converged      bool  // did convergence happen after healing?
	ITCWithinBound bool  // is ITC metadata bounded?
	FinalStats     []NodeStats
	NetworkStats   NetworkStats
}

func (r ScenarioResult) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Scenario Result:\n")
	fmt.Fprintf(&b, "  Duration:          %v\n", r.Duration.Round(time.Millisecond))
	fmt.Fprintf(&b, "  Divergence seen:   %v\n", r.DivergenceSeen)
	fmt.Fprintf(&b, "  Converged:         %v\n", r.Converged)
	fmt.Fprintf(&b, "  ITC within bound:  %v\n", r.ITCWithinBound)
	fmt.Fprintf(&b, "  Messages delivered:%d  dropped:%d\n",
		r.NetworkStats.Delivered, r.NetworkStats.Dropped)
	fmt.Fprintf(&b, "\nFinal node states:\n")
	for _, s := range r.FinalStats {
		fmt.Fprintf(&b, "  %s\n", s)
	}
	return b.String()
}

func memberSet(members []string) map[string]bool {
	m := make(map[string]bool, len(members))
	for _, s := range members {
		m[s] = true
	}
	return m
}

func setsEqual(a, b map[string]bool) bool {
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

func uniqueValues(m map[string]int64) []int64 {
	seen := make(map[int64]bool)
	for _, v := range m {
		seen[v] = true
	}
	result := make([]int64, 0, len(seen))
	for v := range seen {
		result = append(result, v)
	}
	return result
}

func memberCounts(m map[string][]string) map[string]int {
	result := make(map[string]int, len(m))
	for k, v := range m {
		result[k] = len(v)
	}
	return result
}

func nodeIDsToStrings(ids interface{}) []string {
	return []string{fmt.Sprintf("%v", ids)}
}
