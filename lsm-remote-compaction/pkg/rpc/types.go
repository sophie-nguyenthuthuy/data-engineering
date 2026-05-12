package rpc

// SSTFile carries an SSTable file's bytes inline over the wire.
type SSTFile struct {
	Path  string `json:"path"`
	Data  []byte `json:"data"`
	Level int32  `json:"level"`
}

// CompactionRequest initiates a remote compaction job.
type CompactionRequest struct {
	CompactionID  string    `json:"compaction_id"`
	InputFiles    []SSTFile `json:"input_files"`
	TargetLevel   int32     `json:"target_level"`
	PeerAddresses []string  `json:"peer_addresses"`
	QuorumSize    int32     `json:"quorum_size"`
}

// CompactionResponse acknowledges job receipt.
type CompactionResponse struct {
	CompactionID string `json:"compaction_id"`
	Status       string `json:"status"` // "accepted" | "error"
	Error        string `json:"error,omitempty"`
}

// StatusRequest polls a running job.
type StatusRequest struct {
	CompactionID string `json:"compaction_id"`
}

// StatusResponse describes job state.
type StatusResponse struct {
	CompactionID string   `json:"compaction_id"`
	Status       string   `json:"status"` // "running" | "done" | "error"
	OutputFile   *SSTFile `json:"output_file,omitempty"`
	Error        string   `json:"error,omitempty"`
}

// AckRequest is sent by a peer during quorum voting.
type AckRequest struct {
	CompactionID string `json:"compaction_id"`
	NodeID       string `json:"node_id"`
}

// AckResponse carries the peer's vote.
type AckResponse struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// CommitRequest finalises a job after quorum is satisfied.
type CommitRequest struct {
	CompactionID string `json:"compaction_id"`
}

// CommitResponse confirms the commit.
type CommitResponse struct {
	Committed bool   `json:"committed"`
	Error     string `json:"error,omitempty"`
}
