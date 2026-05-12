package quorum

import (
	"sync/atomic"
	"time"
)

// Metrics tracks operation counts and latencies for the KV metadata store.
// It is safe for concurrent use (all counters are atomic).
type Metrics struct {
	reads    atomic.Int64
	writes   atomic.Int64
	readNs   atomic.Int64 // cumulative read latency in nanoseconds
	writeNs  atomic.Int64
	lastReset time.Time
}

func NewMetrics() *Metrics {
	return &Metrics{lastReset: time.Now()}
}

func (m *Metrics) RecordRead(d time.Duration) {
	m.reads.Add(1)
	m.readNs.Add(int64(d))
}

func (m *Metrics) RecordWrite(d time.Duration) {
	m.writes.Add(1)
	m.writeNs.Add(int64(d))
}

// ReadWriteRatio returns reads/writes over the measurement window.
// Returns 1.0 (balanced) if no writes have been observed.
func (m *Metrics) ReadWriteRatio() float64 {
	w := m.writes.Load()
	r := m.reads.Load()
	if w == 0 {
		return 1.0
	}
	return float64(r) / float64(w)
}

// Snapshot returns a point-in-time copy of all counters.
func (m *Metrics) Snapshot() MetricsSnapshot {
	r := m.reads.Load()
	w := m.writes.Load()
	rns := m.readNs.Load()
	wns := m.writeNs.Load()
	var avgRead, avgWrite time.Duration
	if r > 0 {
		avgRead = time.Duration(rns / r)
	}
	if w > 0 {
		avgWrite = time.Duration(wns / w)
	}
	return MetricsSnapshot{
		Reads:          r,
		Writes:         w,
		AvgReadLatency: avgRead,
		AvgWriteLatency: avgWrite,
		RWRatio:        m.ReadWriteRatio(),
		Window:         time.Since(m.lastReset),
	}
}

func (m *Metrics) Reset() {
	m.reads.Store(0)
	m.writes.Store(0)
	m.readNs.Store(0)
	m.writeNs.Store(0)
	m.lastReset = time.Now()
}

type MetricsSnapshot struct {
	Reads           int64
	Writes          int64
	AvgReadLatency  time.Duration
	AvgWriteLatency time.Duration
	RWRatio         float64
	Window          time.Duration
}
