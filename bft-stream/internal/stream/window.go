package stream

import (
	"math"
	"sync"
	"time"
)

// windowState accumulates records for a single tumbling window.
type windowState struct {
	id      WindowID
	count   int64
	sum     float64
	min     float64
	max     float64
	sealed  bool // true after consensus commits the watermark
}

func newWindowState(id WindowID) *windowState {
	return &windowState{id: id, min: math.MaxFloat64, max: -math.MaxFloat64}
}

func (w *windowState) add(v float64) {
	w.count++
	w.sum += v
	if v < w.min {
		w.min = v
	}
	if v > w.max {
		w.max = v
	}
}

func (w *windowState) result(committedAt time.Time) WindowResult {
	mean := 0.0
	if w.count > 0 {
		mean = w.sum / float64(w.count)
	}
	return WindowResult{
		WindowID:    w.id,
		Count:       w.count,
		Sum:         w.sum,
		Min:         w.min,
		Max:         w.max,
		Mean:        mean,
		CommittedAt: committedAt,
		Latency:     committedAt.Sub(w.id.End),
	}
}

// WindowManager maintains open tumbling windows and routes incoming records.
type WindowManager struct {
	mu       sync.Mutex
	size     time.Duration
	windows  map[time.Time]*windowState // keyed by window Start
	watermark time.Time
}

// NewWindowManager creates a manager for tumbling windows of the given size.
func NewWindowManager(size time.Duration) *WindowManager {
	return &WindowManager{
		size:    size,
		windows: make(map[time.Time]*windowState),
	}
}

// windowFor returns (or creates) the window that owns the given event time.
func (wm *WindowManager) windowFor(t time.Time) *windowState {
	start := t.Truncate(wm.size)
	w, ok := wm.windows[start]
	if !ok {
		w = newWindowState(WindowID{Start: start, End: start.Add(wm.size)})
		wm.windows[start] = w
	}
	return w
}

// Add routes a record to the appropriate window. Records whose event time falls
// before the current watermark are late arrivals and are dropped (with a
// warning in production this would go to a side output).
func (wm *WindowManager) Add(r Record) bool {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	if !wm.watermark.IsZero() && r.EventTime.Before(wm.watermark) {
		return false // late; dropped
	}
	w := wm.windowFor(r.EventTime)
	if w.sealed {
		return false
	}
	w.add(r.Value)
	return true
}

// ProposeWatermark returns a WatermarkProposal for the window that ends at or
// before proposedWM. Returns false if no such window exists or it's already
// sealed or there are no records.
func (wm *WindowManager) ProposeWatermark(proposedWM time.Time) (WatermarkProposal, bool) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Find the oldest unsealed window whose End <= proposedWM
	var target *windowState
	for _, w := range wm.windows {
		if w.sealed {
			continue
		}
		if !w.id.End.After(proposedWM) {
			if target == nil || w.id.Start.Before(target.id.Start) {
				target = w
			}
		}
	}
	if target == nil || target.count == 0 {
		return WatermarkProposal{}, false
	}
	return WatermarkProposal{
		WindowID:     target.id,
		NewWatermark: target.id.End,
		RecordCount:  target.count,
		Checksum:     target.sum,
	}, true
}

// Commit seals the window matching the proposal and returns its result.
// It also advances the internal watermark. Returns false if the window was
// already sealed (idempotent commit).
func (wm *WindowManager) Commit(p WatermarkProposal, at time.Time) (WindowResult, bool) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	w, ok := wm.windows[p.WindowID.Start]
	if !ok || w.sealed {
		return WindowResult{}, false
	}
	w.sealed = true
	if p.NewWatermark.After(wm.watermark) {
		wm.watermark = p.NewWatermark
	}
	return w.result(at), true
}

// Watermark returns the current committed watermark.
func (wm *WindowManager) Watermark() time.Time {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.watermark
}

// PendingWindowCount returns the number of open (unsealed) windows.
func (wm *WindowManager) PendingWindowCount() int {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	count := 0
	for _, w := range wm.windows {
		if !w.sealed {
			count++
		}
	}
	return count
}
