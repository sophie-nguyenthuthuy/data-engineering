// Package stream implements event-time windowed aggregation on top of a
// pluggable consensus layer. Only watermark advancement events go through
// consensus — individual records are processed locally on each replica.
package stream

import (
	"encoding/json"
	"time"
)

// Record is an event in the stream. Each replica receives every record via
// reliable broadcast (no consensus required per record).
type Record struct {
	Key       string
	Value     float64
	EventTime time.Time
	// SeqID is a monotonically increasing per-producer sequence number used
	// to detect gaps in delivery.
	SeqID uint64
}

// WindowID uniquely identifies a tumbling window by its closed-open interval.
type WindowID struct {
	Start time.Time
	End   time.Time
}

func (w WindowID) String() string {
	return w.Start.Format("15:04:05.000") + "→" + w.End.Format("15:04:05.000")
}

// WindowResult is the output of a finalized window aggregate.
type WindowResult struct {
	WindowID WindowID
	Count    int64
	Sum      float64
	Min      float64
	Max      float64
	Mean     float64
	// CommittedAt records when consensus finalized this window.
	CommittedAt time.Time
	// Latency is the time from WindowID.End to CommittedAt.
	Latency time.Duration
}

// WatermarkProposal is the value submitted to the consensus layer.
// It proposes advancing the stream watermark to a new time, which closes
// all windows whose End <= NewWatermark.
//
// Only the stream primary proposes watermarks; all replicas validate that:
//  1. NewWatermark > current watermark (monotone)
//  2. MaxRecordSeq matches expected number of records (prevents skipping)
type WatermarkProposal struct {
	WindowID     WindowID
	NewWatermark time.Time
	RecordCount  int64   // records seen in this window
	Checksum     float64 // sum of all values — cross-checked by honest replicas
}

// Encode serializes the proposal for transmission through the consensus layer.
func (p WatermarkProposal) Encode() ([]byte, error) {
	return json.Marshal(p)
}

// DecodeWatermarkProposal deserializes a proposal.
func DecodeWatermarkProposal(b []byte) (WatermarkProposal, error) {
	var p WatermarkProposal
	err := json.Unmarshal(b, &p)
	return p, err
}
