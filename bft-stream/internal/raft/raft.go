// Package raft implements a simplified Raft consensus protocol for use as the
// CFT (Crash Fault-Tolerant) baseline in benchmarks. It supports the core
// Raft operations: leader election and log replication. Snapshotting and
// membership changes are omitted for brevity.
//
// Key difference from PBFT: Raft requires only a simple majority (f+1 of 2f+1
// nodes), whereas PBFT requires 2f+1 of 3f+1. This means Raft sends O(n)
// messages per slot versus PBFT's O(n²), which is the primary source of the
// throughput gap.
package raft

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/sophie-nguyenthuthuy/bft-stream/internal/transport"
)

// ---- message types ---------------------------------------------------------

const (
	MsgRequestVote    = "raft:requestvote"
	MsgRequestVoteRes = "raft:requestvoteres"
	MsgAppendEntries  = "raft:appendentries"
	MsgAppendRes      = "raft:appendres"
	MsgClientRequest  = "raft:client"
)

type requestVoteMsg struct {
	Term         int64
	CandidateID  int
	LastLogIndex int64
	LastLogTerm  int64
}

type requestVoteRes struct {
	Term        int64
	VoteGranted bool
	VoterID     int
}

type appendEntriesMsg struct {
	Term         int64
	LeaderID     int
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []logEntry
	LeaderCommit int64
}

type appendEntriesRes struct {
	Term      int64
	Success   bool
	FollowerID int
	MatchIndex int64
}

type clientRequestMsg struct {
	Value []byte
}

// ---- log entry -------------------------------------------------------------

type logEntry struct {
	Term  int64
	Index int64
	Value []byte
}

// ---- Node ------------------------------------------------------------------

type role int

const (
	follower  role = iota
	candidate
	leader
)

// CommitCallback is called when an entry is applied to the state machine.
type CommitCallback func(seq uint64, value []byte)

// Node is a single Raft replica.
type Node struct {
	id  int
	n   int
	bus *transport.Bus

	mu          sync.Mutex
	role        role
	currentTerm int64
	votedFor    int // -1 = none
	log         []logEntry
	commitIndex int64
	lastApplied int64

	// Leader state
	nextIndex  map[int]int64
	matchIndex map[int]int64
	votes      map[int64]map[int]bool // term → voterID → bool

	onCommit CommitCallback

	// Pending proposals: index → done chan
	proposals map[int64]chan commitResult
	propMu    sync.Mutex

	electionTimer  *time.Timer
	heartbeatTimer *time.Timer

	rng *rand.Rand

	stopCh chan struct{}
}

type commitResult struct {
	seq uint64
	err error
}

const (
	heartbeatInterval       = 50 * time.Millisecond
	electionTimeoutMin      = 150 * time.Millisecond
	electionTimeoutRange    = 150 * time.Millisecond
)

// New creates a Raft node.
func New(id, n int, bus *transport.Bus, onCommit CommitCallback) *Node {
	nd := &Node{
		id:         id,
		n:          n,
		bus:        bus,
		role:       follower,
		votedFor:   -1,
		log:        []logEntry{{Term: 0, Index: 0}}, // sentinel at index 0
		nextIndex:  make(map[int]int64),
		matchIndex: make(map[int]int64),
		votes:      make(map[int64]map[int]bool),
		proposals:  make(map[int64]chan commitResult),
		onCommit:   onCommit,
		rng:        rand.New(rand.NewSource(int64(id) * 7919)),
		stopCh:     make(chan struct{}),
	}
	bus.Register(id, nd.handleMsg)
	return nd
}

// Start begins background timers.
func (nd *Node) Start() {
	nd.mu.Lock()
	nd.resetElectionTimer()
	nd.mu.Unlock()
}

// Stop stops the node.
func (nd *Node) Stop() {
	close(nd.stopCh)
	nd.mu.Lock()
	if nd.electionTimer != nil {
		nd.electionTimer.Stop()
	}
	if nd.heartbeatTimer != nil {
		nd.heartbeatTimer.Stop()
	}
	nd.mu.Unlock()
}

// IsLeader returns true if this node currently believes it is the leader.
func (nd *Node) IsLeader() bool {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return nd.role == leader
}

// ID returns the node identifier.
func (nd *Node) ID() int { return nd.id }

// Propose appends a value to the replicated log and blocks until committed.
// If not the leader, it forwards to all nodes (who will ignore if not leader).
func (nd *Node) Propose(value []byte) (uint64, error) {
	nd.mu.Lock()
	if nd.role != leader {
		nd.mu.Unlock()
		// Forward to all; the actual leader will handle it
		nd.bus.Send(transport.Msg{From: nd.id, To: -1, Type: MsgClientRequest,
			Payload: mustMarshal(clientRequestMsg{Value: value})})
		// Busy-wait for someone to become leader and commit
		// (simplified; real clients would retry with the leader)
		doneCh := make(chan commitResult, 1)
		go func() {
			for {
				select {
				case <-nd.stopCh:
					return
				case <-time.After(10 * time.Millisecond):
					nd.mu.Lock()
					isLeader := nd.role == leader
					nd.mu.Unlock()
					if isLeader {
						seq, err := nd.Propose(value)
						doneCh <- commitResult{seq: seq, err: err}
						return
					}
				}
			}
		}()
		res := <-doneCh
		return res.seq, res.err
	}

	// Append to log
	idx := int64(len(nd.log))
	entry := logEntry{Term: nd.currentTerm, Index: idx, Value: value}
	nd.log = append(nd.log, entry)
	doneCh := make(chan commitResult, 1)
	nd.propMu.Lock()
	nd.proposals[idx] = doneCh
	nd.propMu.Unlock()
	nd.mu.Unlock()

	nd.sendAppendEntries()

	select {
	case res := <-doneCh:
		return res.seq, res.err
	case <-nd.stopCh:
		return 0, nil
	}
}

// ---- message handler -------------------------------------------------------

func (nd *Node) handleMsg(msg transport.Msg) {
	select {
	case <-nd.stopCh:
		return
	default:
	}

	switch msg.Type {
	case MsgRequestVote:
		var m requestVoteMsg
		if err := json.Unmarshal(msg.Payload, &m); err != nil {
			return
		}
		nd.handleRequestVote(msg.From, m)

	case MsgRequestVoteRes:
		var m requestVoteRes
		if err := json.Unmarshal(msg.Payload, &m); err != nil {
			return
		}
		nd.handleVoteResponse(m)

	case MsgAppendEntries:
		var m appendEntriesMsg
		if err := json.Unmarshal(msg.Payload, &m); err != nil {
			return
		}
		nd.handleAppendEntries(msg.From, m)

	case MsgAppendRes:
		var m appendEntriesRes
		if err := json.Unmarshal(msg.Payload, &m); err != nil {
			return
		}
		nd.handleAppendResponse(m)

	case MsgClientRequest:
		var m clientRequestMsg
		if err := json.Unmarshal(msg.Payload, &m); err != nil {
			return
		}
		nd.mu.Lock()
		if nd.role == leader {
			nd.mu.Unlock()
			nd.Propose(m.Value) //nolint
		} else {
			nd.mu.Unlock()
		}
	}
}

func (nd *Node) handleRequestVote(from int, m requestVoteMsg) {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	if m.Term > nd.currentTerm {
		nd.becomeFollower(m.Term)
	}

	lastIdx := int64(len(nd.log) - 1)
	lastTerm := nd.log[lastIdx].Term
	logOK := m.LastLogTerm > lastTerm || (m.LastLogTerm == lastTerm && m.LastLogIndex >= lastIdx)
	grant := m.Term == nd.currentTerm && logOK && (nd.votedFor == -1 || nd.votedFor == m.CandidateID)

	if grant {
		nd.votedFor = m.CandidateID
		nd.resetElectionTimer()
	}

	nd.bus.Send(transport.Msg{
		From: nd.id, To: from, Type: MsgRequestVoteRes,
		Payload: mustMarshal(requestVoteRes{Term: nd.currentTerm, VoteGranted: grant, VoterID: nd.id}),
	})
}

func (nd *Node) handleVoteResponse(m requestVoteRes) {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	if m.Term > nd.currentTerm {
		nd.becomeFollower(m.Term)
		return
	}
	if nd.role != candidate || m.Term != nd.currentTerm {
		return
	}
	if !m.VoteGranted {
		return
	}

	if nd.votes[nd.currentTerm] == nil {
		nd.votes[nd.currentTerm] = make(map[int]bool)
	}
	nd.votes[nd.currentTerm][m.VoterID] = true

	if len(nd.votes[nd.currentTerm]) >= (nd.n/2)+1 {
		nd.becomeLeader()
	}
}

func (nd *Node) handleAppendEntries(from int, m appendEntriesMsg) {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	success := false
	matchIndex := int64(0)

	if m.Term >= nd.currentTerm {
		nd.becomeFollower(m.Term)
		nd.resetElectionTimer()

		prevOK := m.PrevLogIndex == 0 ||
			(m.PrevLogIndex < int64(len(nd.log)) &&
				nd.log[m.PrevLogIndex].Term == m.PrevLogTerm)

		if prevOK {
			success = true
			for _, entry := range m.Entries {
				idx := entry.Index
				if idx < int64(len(nd.log)) {
					if nd.log[idx].Term != entry.Term {
						nd.log = nd.log[:idx]
						nd.log = append(nd.log, entry)
					}
				} else {
					nd.log = append(nd.log, entry)
				}
			}
			if len(m.Entries) > 0 {
				matchIndex = m.Entries[len(m.Entries)-1].Index
			} else {
				matchIndex = m.PrevLogIndex
			}
			if m.LeaderCommit > nd.commitIndex {
				nd.commitIndex = min64(m.LeaderCommit, int64(len(nd.log)-1))
				nd.applyCommitted()
			}
		}
	}

	nd.bus.Send(transport.Msg{
		From: nd.id, To: from, Type: MsgAppendRes,
		Payload: mustMarshal(appendEntriesRes{
			Term: nd.currentTerm, Success: success,
			FollowerID: nd.id, MatchIndex: matchIndex,
		}),
	})
}

func (nd *Node) handleAppendResponse(m appendEntriesRes) {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	if m.Term > nd.currentTerm {
		nd.becomeFollower(m.Term)
		return
	}
	if nd.role != leader {
		return
	}
	if m.Success {
		nd.matchIndex[m.FollowerID] = m.MatchIndex
		nd.nextIndex[m.FollowerID] = m.MatchIndex + 1
		nd.maybeAdvanceCommit()
	} else {
		if nd.nextIndex[m.FollowerID] > 1 {
			nd.nextIndex[m.FollowerID]--
		}
	}
}

// ---- role transitions ------------------------------------------------------

func (nd *Node) becomeFollower(term int64) {
	nd.role = follower
	nd.currentTerm = term
	nd.votedFor = -1
	if nd.heartbeatTimer != nil {
		nd.heartbeatTimer.Stop()
	}
}

func (nd *Node) becomeCandidate() {
	nd.role = candidate
	nd.currentTerm++
	nd.votedFor = nd.id
	if nd.votes[nd.currentTerm] == nil {
		nd.votes[nd.currentTerm] = make(map[int]bool)
	}
	nd.votes[nd.currentTerm][nd.id] = true // self-vote

	lastIdx := int64(len(nd.log) - 1)
	lastTerm := nd.log[lastIdx].Term
	nd.bus.Send(transport.Msg{
		From: nd.id, To: -1, Type: MsgRequestVote,
		Payload: mustMarshal(requestVoteMsg{
			Term: nd.currentTerm, CandidateID: nd.id,
			LastLogIndex: lastIdx, LastLogTerm: lastTerm,
		}),
	})
	nd.resetElectionTimer()

	// Check if already have quorum (single-node cluster)
	if len(nd.votes[nd.currentTerm]) >= (nd.n/2)+1 {
		nd.becomeLeader()
	}
}

func (nd *Node) becomeLeader() {
	nd.role = leader
	if nd.electionTimer != nil {
		nd.electionTimer.Stop()
	}
	lastIdx := int64(len(nd.log) - 1)
	for i := 0; i < nd.n; i++ {
		nd.nextIndex[i] = lastIdx + 1
		nd.matchIndex[i] = 0
	}
	nd.heartbeatTimer = time.AfterFunc(heartbeatInterval, nd.sendHeartbeats)
}

// ---- log replication -------------------------------------------------------

func (nd *Node) sendHeartbeats() {
	nd.mu.Lock()
	if nd.role != leader {
		nd.mu.Unlock()
		return
	}
	nd.mu.Unlock()
	nd.sendAppendEntries()
	nd.mu.Lock()
	if nd.role == leader {
		nd.heartbeatTimer.Reset(heartbeatInterval)
	}
	nd.mu.Unlock()
}

type appendTask struct {
	to           int
	prevIdx      int64
	prevTerm     int64
	entries      []logEntry
	leaderCommit int64
	term         int64
	leaderID     int
}

func (nd *Node) sendAppendEntries() {
	nd.mu.Lock()
	if nd.role != leader {
		nd.mu.Unlock()
		return
	}
	tasks := make([]appendTask, 0, nd.n)
	for i := 0; i < nd.n; i++ {
		if i == nd.id {
			continue
		}
		nextIdx := nd.nextIndex[i]
		if nextIdx < 1 {
			nextIdx = 1
		}
		prevIdx := nextIdx - 1
		var prevTerm int64
		if prevIdx < int64(len(nd.log)) {
			prevTerm = nd.log[prevIdx].Term
		}
		var entries []logEntry
		if nextIdx < int64(len(nd.log)) {
			src := nd.log[nextIdx:]
			entries = make([]logEntry, len(src))
			copy(entries, src)
		}
		tasks = append(tasks, appendTask{
			to: i, prevIdx: prevIdx, prevTerm: prevTerm,
			entries: entries, leaderCommit: nd.commitIndex,
			term: nd.currentTerm, leaderID: nd.id,
		})
	}
	nd.mu.Unlock()

	for _, t := range tasks {
		t := t
		go nd.bus.Send(transport.Msg{
			From: nd.id, To: t.to, Type: MsgAppendEntries,
			Payload: mustMarshal(appendEntriesMsg{
				Term: t.term, LeaderID: t.leaderID,
				PrevLogIndex: t.prevIdx, PrevLogTerm: t.prevTerm,
				Entries: t.entries, LeaderCommit: t.leaderCommit,
			}),
		})
	}
}

func (nd *Node) maybeAdvanceCommit() {
	// Find highest index replicated on a majority
	for n := int64(len(nd.log)) - 1; n > nd.commitIndex; n-- {
		if nd.log[n].Term != nd.currentTerm {
			continue
		}
		count := 1 // self
		for i := 0; i < nd.n; i++ {
			if i != nd.id && nd.matchIndex[i] >= n {
				count++
			}
		}
		if count >= (nd.n/2)+1 {
			nd.commitIndex = n
			nd.applyCommitted()
			break
		}
	}
}

func (nd *Node) applyCommitted() {
	for nd.lastApplied < nd.commitIndex {
		nd.lastApplied++
		entry := nd.log[nd.lastApplied]
		val := entry.Value
		seq := uint64(nd.lastApplied)

		nd.propMu.Lock()
		ch := nd.proposals[entry.Index]
		delete(nd.proposals, entry.Index)
		nd.propMu.Unlock()

		if ch != nil {
			ch <- commitResult{seq: seq}
		}
		if nd.onCommit != nil {
			go nd.onCommit(seq, val)
		}
	}
}

// ---- timers ----------------------------------------------------------------

func (nd *Node) resetElectionTimer() {
	timeout := electionTimeoutMin + time.Duration(nd.rng.Int63n(int64(electionTimeoutRange)))
	if nd.electionTimer == nil {
		nd.electionTimer = time.AfterFunc(timeout, func() {
			nd.mu.Lock()
			if nd.role != leader {
				nd.becomeCandidate()
			}
			nd.mu.Unlock()
		})
	} else {
		nd.electionTimer.Reset(timeout)
	}
}

// ---- Cluster ---------------------------------------------------------------

// Cluster manages a group of Raft nodes.
type Cluster struct {
	Nodes []*Node
	Bus   *transport.Bus
}

// NewCluster creates a Raft cluster of n nodes.
func NewCluster(n int, onCommit func(nodeID int, seq uint64, value []byte)) *Cluster {
	bus := transport.NewBus()
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		i := i
		cb := func(seq uint64, value []byte) {
			if onCommit != nil {
				onCommit(i, seq, value)
			}
		}
		nodes[i] = New(i, n, bus, cb)
	}
	return &Cluster{Nodes: nodes, Bus: bus}
}

// Start starts all nodes.
func (c *Cluster) Start() {
	for _, nd := range c.Nodes {
		nd.Start()
	}
}

// Stop stops all nodes.
func (c *Cluster) Stop() {
	for _, nd := range c.Nodes {
		nd.Stop()
	}
}

// Leader returns the current leader or blocks until one is elected.
func (c *Cluster) Leader() *Node {
	for i := 0; i < 200; i++ {
		for _, nd := range c.Nodes {
			if nd.IsLeader() {
				return nd
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return c.Nodes[0]
}

// ---- helpers ---------------------------------------------------------------

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
