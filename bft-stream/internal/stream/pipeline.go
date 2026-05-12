package stream

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ConsensusNode is the interface both PBFT and Raft nodes satisfy.
type ConsensusNode interface {
	Propose(value []byte) (uint64, error)
	IsLeader() bool
	ID() int
}

// Pipeline is a streaming processor that uses a consensus node to agree on
// watermark advancement. Each replica in the cluster runs an identical Pipeline
// connected to its own consensus node; they share state only through the
// consensus protocol.
//
// Design principle: individual records bypass consensus entirely. Only the
// "close this window" event goes through BFT/Raft. All honest replicas that
// have received the same records will produce identical aggregates, so
// consensus on the watermark is sufficient to guarantee correctness.
type Pipeline struct {
	nodeID    int
	consensus ConsensusNode
	wm        *WindowManager

	resultsCh  chan WindowResult
	onResult   func(WindowResult)

	// Stats
	recordsIn     atomic.Int64
	windowsOut    atomic.Int64
	lateDropped   atomic.Int64
	consensusOps  atomic.Int64

	mu            sync.Mutex
	watermarkTick *time.Ticker
	stopCh        chan struct{}
}

// New creates a pipeline for the given replica node.
func New(nodeID int, windowSize time.Duration, consensus ConsensusNode) *Pipeline {
	return &Pipeline{
		nodeID:    nodeID,
		consensus: consensus,
		wm:        NewWindowManager(windowSize),
		resultsCh: make(chan WindowResult, 256),
		stopCh:    make(chan struct{}),
	}
}

// OnResult registers a callback invoked for each committed window result.
func (p *Pipeline) OnResult(fn func(WindowResult)) {
	p.onResult = fn
}

// Results returns a channel of committed window results (for the primary only;
// other replicas apply via OnCommit from the consensus layer).
func (p *Pipeline) Results() <-chan WindowResult {
	return p.resultsCh
}

// Start begins the watermark advancement loop. The watermarkInterval controls
// how often the primary proposes a new watermark; shorter intervals mean lower
// latency but more consensus overhead.
func (p *Pipeline) Start(ctx context.Context, watermarkInterval time.Duration) {
	p.watermarkTick = time.NewTicker(watermarkInterval)
	go p.watermarkLoop(ctx)
}

// Stop shuts down the pipeline.
func (p *Pipeline) Stop() {
	close(p.stopCh)
	if p.watermarkTick != nil {
		p.watermarkTick.Stop()
	}
}

// Ingest adds a record to the local window state. This is called on EVERY
// replica directly — no consensus involved for individual records.
func (p *Pipeline) Ingest(r Record) {
	p.recordsIn.Add(1)
	if !p.wm.Add(r) {
		p.lateDropped.Add(1)
	}
}

// ApplyWatermark is called by the consensus commit callback to finalize a
// window on this replica. All replicas (primary and backups) call this when
// the consensus layer commits a WatermarkProposal.
func (p *Pipeline) ApplyWatermark(seq uint64, payload []byte) {
	proposal, err := DecodeWatermarkProposal(payload)
	if err != nil {
		return
	}
	result, ok := p.wm.Commit(proposal, time.Now())
	if !ok {
		return // already committed (idempotent)
	}
	p.windowsOut.Add(1)
	p.consensusOps.Add(1)

	if p.onResult != nil {
		p.onResult(result)
	}
	select {
	case p.resultsCh <- result:
	default:
	}
}

// watermarkLoop runs on the primary replica, periodically proposing watermark
// advancement through the consensus layer.
func (p *Pipeline) watermarkLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-p.watermarkTick.C:
			if !p.consensus.IsLeader() {
				continue
			}
			p.proposeWatermark()
		}
	}
}

func (p *Pipeline) proposeWatermark() {
	// Propose watermark = now - small lag to allow records to arrive
	// In production this would track per-source low watermarks
	proposed := time.Now().Add(-10 * time.Millisecond)

	prop, ok := p.wm.ProposeWatermark(proposed)
	if !ok {
		return
	}
	payload, err := prop.Encode()
	if err != nil {
		return
	}
	// This is the ONLY consensus operation per window.
	// BFT protects against a Byzantine primary that tries to:
	//  - skip records (wrong RecordCount/Checksum)
	//  - advance watermark too aggressively (honest replicas reject)
	//  - propose watermarks out of order
	_, _ = p.consensus.Propose(payload)
}

// Stats returns current pipeline statistics.
func (p *Pipeline) Stats() PipelineStats {
	return PipelineStats{
		NodeID:       p.nodeID,
		RecordsIn:    p.recordsIn.Load(),
		WindowsOut:   p.windowsOut.Load(),
		LateDropped:  p.lateDropped.Load(),
		ConsensusOps: p.consensusOps.Load(),
	}
}

// PipelineStats holds runtime counters for a single replica.
type PipelineStats struct {
	NodeID       int
	RecordsIn    int64
	WindowsOut   int64
	LateDropped  int64
	ConsensusOps int64
}
