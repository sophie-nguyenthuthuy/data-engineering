// Package pbft implements Practical Byzantine Fault Tolerance (Castro & Liskov 1999).
//
// The protocol proceeds in three phases for each consensus slot:
//
//  PRE-PREPARE  Primary → All  : assign sequence number n to client request
//  PREPARE      All → All      : confirm receipt; proves 2f+1 saw the assignment
//  COMMIT       All → All      : proves 2f+1 are prepared; safe to execute
//
// A value is committed when a node collects 2f+1 COMMIT messages for the same
// (view, seq, digest). With n = 3f+1 nodes this tolerates f Byzantine faults.
//
// View changes are triggered by a timeout on the primary. This implementation
// elects the next primary as (view+1) mod n — a simple rotating scheme.
package pbft

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/sophie-nguyenthuthuy/bft-stream/internal/transport"
)

// ---- message types ---------------------------------------------------------

const (
	MsgRequest    = "pbft:request"
	MsgPrePrepare = "pbft:preprepare"
	MsgPrepare    = "pbft:prepare"
	MsgCommit     = "pbft:commit"
	MsgViewChange = "pbft:viewchange"
	MsgNewView    = "pbft:newview"
)

type requestMsg struct {
	Op        []byte
	Timestamp int64
	ClientID  int
}

type prePrepareMsg struct {
	View   int64
	Seq    int64
	Digest [32]byte
	// Inline request so backups can verify digest
	Request requestMsg
}

type prepareMsg struct {
	View   int64
	Seq    int64
	Digest [32]byte
	NodeID int
}

type commitMsg struct {
	View   int64
	Seq    int64
	Digest [32]byte
	NodeID int
}

type viewChangeMsg struct {
	NewView int64
	NodeID  int
	LastSeq int64
}

type newViewMsg struct {
	View    int64
	Primary int
}

// ---- log entry -------------------------------------------------------------

type logEntry struct {
	view     int64
	seq      int64
	digest   [32]byte
	value    []byte
	prepares map[int]bool // nodeID → true
	commits  map[int]bool
}

func newLogEntry(view, seq int64, digest [32]byte, value []byte) *logEntry {
	return &logEntry{
		view:     view,
		seq:      seq,
		digest:   digest,
		value:    value,
		prepares: make(map[int]bool),
		commits:  make(map[int]bool),
	}
}

func (e *logEntry) isPrepared(f int) bool {
	return len(e.prepares) >= 2*f
}

func (e *logEntry) isCommitted(f int) bool {
	return len(e.commits) >= 2*f+1
}

// ---- pending proposal ------------------------------------------------------

type proposal struct {
	value  []byte
	done   chan commitResult
}

type commitResult struct {
	seq uint64
	err error
}

// ---- Node ------------------------------------------------------------------

// CommitCallback is invoked when a value is durably committed.
type CommitCallback func(seq uint64, value []byte)

// Node is a single PBFT replica.
type Node struct {
	id   int
	n    int
	f    int // = (n-1)/3
	bus  *transport.Bus

	mu          sync.Mutex
	view        int64
	seq         int64          // last assigned (primary) or last seen
	log         map[int64]*logEntry
	executed    int64          // highest contiguous executed seq
	execQueue   map[int64]bool // committed but waiting for in-order exec

	pendingMu sync.Mutex
	pending   []*proposal // queued proposals (primary only)

	onCommit CommitCallback

	viewChangeMu    sync.Mutex
	viewChangeVotes map[int64]map[int]viewChangeMsg // view → nodeID → msg

	viewTimer  *time.Timer
	viewTimeout time.Duration

	stopCh chan struct{}
	logger *log.Logger
}

// New creates a PBFT node. All nodes in the cluster must share the same *transport.Bus.
func New(id, n int, bus *transport.Bus, onCommit CommitCallback) *Node {
	f := (n - 1) / 3
	nd := &Node{
		id:              id,
		n:               n,
		f:               f,
		bus:             bus,
		log:             make(map[int64]*logEntry),
		execQueue:       make(map[int64]bool),
		onCommit:        onCommit,
		viewChangeVotes: make(map[int64]map[int]viewChangeMsg),
		viewTimeout:     500 * time.Millisecond,
		stopCh:          make(chan struct{}),
		logger:          log.New(log.Writer(), "", 0),
	}
	bus.Register(id, nd.handleMsg)
	return nd
}

// Start begins the node's background timer goroutine.
func (nd *Node) Start() {
	nd.mu.Lock()
	nd.viewTimer = time.AfterFunc(nd.viewTimeout, nd.triggerViewChange)
	nd.mu.Unlock()
}

// Stop signals the node to shut down.
func (nd *Node) Stop() {
	close(nd.stopCh)
	nd.mu.Lock()
	if nd.viewTimer != nil {
		nd.viewTimer.Stop()
	}
	nd.mu.Unlock()
}

// IsLeader returns true when this node is the current primary.
func (nd *Node) IsLeader() bool {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	return nd.primaryForView(nd.view) == nd.id
}

// ID returns the node's identifier.
func (nd *Node) ID() int { return nd.id }

// Propose submits a value for BFT consensus and blocks until committed.
// Only the primary node actually sequences; backups forward to the primary.
func (nd *Node) Propose(value []byte) (uint64, error) {
	p := &proposal{value: value, done: make(chan commitResult, 1)}
	nd.pendingMu.Lock()
	nd.pending = append(nd.pending, p)
	nd.pendingMu.Unlock()

	nd.mu.Lock()
	isPrimary := nd.primaryForView(nd.view) == nd.id
	nd.mu.Unlock()

	if isPrimary {
		nd.drainPending()
	} else {
		// Forward request to current primary
		req := requestMsg{Op: value, Timestamp: time.Now().UnixNano(), ClientID: nd.id}
		nd.broadcast(MsgRequest, req)
	}

	select {
	case res := <-p.done:
		return res.seq, res.err
	case <-nd.stopCh:
		return 0, nil
	}
}

// ---- internal --------------------------------------------------------------

func (nd *Node) primaryForView(view int64) int {
	return int(view) % nd.n
}

func digest(req requestMsg) [32]byte {
	h := sha256.New()
	_ = binary.Write(h, binary.LittleEndian, req.Timestamp)
	_ = binary.Write(h, binary.LittleEndian, int64(req.ClientID))
	h.Write(req.Op)
	var d [32]byte
	copy(d[:], h.Sum(nil))
	return d
}

func (nd *Node) drainPending() {
	nd.pendingMu.Lock()
	pending := nd.pending
	nd.pending = nil
	nd.pendingMu.Unlock()

	for _, p := range pending {
		nd.sequence(p)
	}
}

func (nd *Node) sequence(p *proposal) {
	req := requestMsg{
		Op:        p.value,
		Timestamp: time.Now().UnixNano(),
		ClientID:  nd.id,
	}
	d := digest(req)

	nd.mu.Lock()
	nd.seq++
	seq := nd.seq
	view := nd.view
	entry := newLogEntry(view, seq, d, p.value)
	// Primary counts its own prepare
	entry.prepares[nd.id] = true
	nd.log[seq] = entry
	nd.resetViewTimer()
	nd.mu.Unlock()

	nd.broadcast(MsgPrePrepare, prePrepareMsg{
		View:    view,
		Seq:     seq,
		Digest:  d,
		Request: req,
	})

	// Watch for this entry being committed so we can unblock Propose
	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				nd.mu.Lock()
				e, ok := nd.log[seq]
				committed := ok && e.isCommitted(nd.f)
				nd.mu.Unlock()
				if committed {
					p.done <- commitResult{seq: uint64(seq)}
					return
				}
			case <-nd.stopCh:
				return
			}
		}
	}()
}

func (nd *Node) handleMsg(msg transport.Msg) {
	select {
	case <-nd.stopCh:
		return
	default:
	}

	switch msg.Type {
	case MsgRequest:
		var req requestMsg
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			return
		}
		nd.mu.Lock()
		isPrimary := nd.primaryForView(nd.view) == nd.id
		nd.mu.Unlock()
		if isPrimary {
			p := &proposal{value: req.Op, done: make(chan commitResult, 1)}
			nd.pendingMu.Lock()
			nd.pending = append(nd.pending, p)
			nd.pendingMu.Unlock()
			nd.drainPending()
		}

	case MsgPrePrepare:
		var pp prePrepareMsg
		if err := json.Unmarshal(msg.Payload, &pp); err != nil {
			return
		}
		nd.handlePrePrepare(msg.From, pp)

	case MsgPrepare:
		var pm prepareMsg
		if err := json.Unmarshal(msg.Payload, &pm); err != nil {
			return
		}
		nd.handlePrepare(pm)

	case MsgCommit:
		var cm commitMsg
		if err := json.Unmarshal(msg.Payload, &cm); err != nil {
			return
		}
		nd.handleCommit(cm)

	case MsgViewChange:
		var vc viewChangeMsg
		if err := json.Unmarshal(msg.Payload, &vc); err != nil {
			return
		}
		nd.handleViewChange(vc)

	case MsgNewView:
		var nv newViewMsg
		if err := json.Unmarshal(msg.Payload, &nv); err != nil {
			return
		}
		nd.handleNewView(nv)
	}
}

func (nd *Node) handlePrePrepare(from int, pp prePrepareMsg) {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	if pp.View != nd.view {
		return
	}
	if nd.primaryForView(pp.View) != from {
		return // not from the primary
	}
	if _, exists := nd.log[pp.Seq]; exists {
		return // already have an entry for this seq
	}
	d := digest(pp.Request)
	if !bytes.Equal(d[:], pp.Digest[:]) {
		return // digest mismatch — Byzantine primary
	}

	entry := newLogEntry(pp.View, pp.Seq, pp.Digest, pp.Request.Op)
	entry.prepares[nd.id] = true
	nd.log[pp.Seq] = entry

	if pp.Seq > nd.seq {
		nd.seq = pp.Seq
	}
	nd.resetViewTimer()

	// Broadcast PREPARE
	pm := prepareMsg{View: pp.View, Seq: pp.Seq, Digest: pp.Digest, NodeID: nd.id}
	go nd.broadcast(MsgPrepare, pm)
}

func (nd *Node) handlePrepare(pm prepareMsg) {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	if pm.View != nd.view {
		return
	}
	entry, ok := nd.log[pm.Seq]
	if !ok {
		return
	}
	if !bytes.Equal(entry.digest[:], pm.Digest[:]) {
		return
	}
	entry.prepares[pm.NodeID] = true

	if entry.isPrepared(nd.f) && !entry.commits[nd.id] {
		entry.commits[nd.id] = true
		cm := commitMsg{View: pm.View, Seq: pm.Seq, Digest: pm.Digest, NodeID: nd.id}
		go nd.broadcast(MsgCommit, cm)
	}
}

func (nd *Node) handleCommit(cm commitMsg) {
	nd.mu.Lock()
	defer nd.mu.Unlock()

	if cm.View != nd.view {
		return
	}
	entry, ok := nd.log[cm.Seq]
	if !ok {
		return
	}
	if !bytes.Equal(entry.digest[:], cm.Digest[:]) {
		return
	}
	entry.commits[cm.NodeID] = true

	if entry.isCommitted(nd.f) {
		nd.maybeExecute()
	}
}

// maybeExecute runs committed entries in sequence order. Must hold nd.mu.
func (nd *Node) maybeExecute() {
	for {
		next := nd.executed + 1
		entry, ok := nd.log[next]
		if !ok || !entry.isCommitted(nd.f) {
			break
		}
		nd.executed = next
		if nd.onCommit != nil {
			val := entry.value
			seq := uint64(next)
			go nd.onCommit(seq, val)
		}
	}
}

// ---- view change -----------------------------------------------------------

func (nd *Node) triggerViewChange() {
	nd.mu.Lock()
	newView := nd.view + 1
	lastSeq := nd.seq
	nd.mu.Unlock()

	vc := viewChangeMsg{NewView: newView, NodeID: nd.id, LastSeq: lastSeq}
	nd.broadcast(MsgViewChange, vc)
	nd.handleViewChange(vc) // self-vote
}

func (nd *Node) handleViewChange(vc viewChangeMsg) {
	nd.viewChangeMu.Lock()
	defer nd.viewChangeMu.Unlock()

	if nd.viewChangeVotes[vc.NewView] == nil {
		nd.viewChangeVotes[vc.NewView] = make(map[int]viewChangeMsg)
	}
	nd.viewChangeVotes[vc.NewView][vc.NodeID] = vc

	votes := len(nd.viewChangeVotes[vc.NewView])
	quorum := 2*nd.f + 1
	if votes >= quorum {
		newPrimary := int(vc.NewView) % nd.n
		nd.mu.Lock()
		if vc.NewView > nd.view {
			nd.view = vc.NewView
			nd.resetViewTimer()
		}
		nd.mu.Unlock()

		if newPrimary == nd.id {
			nd.broadcast(MsgNewView, newViewMsg{View: vc.NewView, Primary: nd.id})
			nd.drainPending()
		}
	}
}

func (nd *Node) handleNewView(nv newViewMsg) {
	nd.mu.Lock()
	defer nd.mu.Unlock()
	if nv.View > nd.view {
		nd.view = nv.View
		nd.resetViewTimer()
	}
}

func (nd *Node) resetViewTimer() {
	if nd.viewTimer != nil {
		nd.viewTimer.Reset(nd.viewTimeout)
	}
}

// ---- helpers ---------------------------------------------------------------

func (nd *Node) broadcast(msgType string, v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	nd.bus.Send(transport.Msg{From: nd.id, To: -1, Type: msgType, Payload: b})
	// Also deliver to self for certain message types
	nd.bus.Send(transport.Msg{From: nd.id, To: nd.id, Type: msgType, Payload: b})
}

// ---- Cluster ---------------------------------------------------------------

// Cluster manages a group of PBFT nodes sharing a transport bus.
type Cluster struct {
	Nodes []*Node
	Bus   *transport.Bus
}

// NewCluster creates n PBFT nodes (n must satisfy n = 3f+1).
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

// Leader returns the current primary node.
func (c *Cluster) Leader() *Node {
	for _, nd := range c.Nodes {
		if nd.IsLeader() {
			return nd
		}
	}
	return c.Nodes[0]
}

// MakeByzantine configures node id to drop a fraction of messages.
func (c *Cluster) MakeByzantine(id int, dropRate float64) {
	c.Bus.SetConfig(id, transport.Config{DropRate: dropRate, Byzantine: true})
}
