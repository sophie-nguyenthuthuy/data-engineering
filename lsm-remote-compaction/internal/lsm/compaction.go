package lsm

// compaction.go — local merge logic consumed by both the local compactor and
// the remote worker.  The remote worker calls MergeEntries directly on data
// it received over gRPC; the local path uses it after taking the appropriate
// level locks.

import (
	"fmt"
	"sort"
)

// MergeEntries merges and deduplicates sorted entry slices from multiple
// SSTables.  It applies tombstones: if a key has Deleted=true in the highest
// sequence (first occurrence in the merged stream from newest→oldest) and the
// target is the bottom level, the tombstone itself is also elided.
//
// Inputs must be pre-sorted by key.  Later slices (higher index) are treated
// as older (lower sequence number).
func MergeEntries(inputs [][]Entry, bottomLevel bool) []Entry {
	// k-way merge using a min-heap
	type cursor struct {
		entries []Entry
		pos     int
		idx     int // input index (0 = newest)
	}
	cursors := make([]cursor, 0, len(inputs))
	for i, es := range inputs {
		if len(es) > 0 {
			cursors = append(cursors, cursor{entries: es, pos: 0, idx: i})
		}
	}

	var merged []Entry
	for len(cursors) > 0 {
		// find cursor with smallest key; break ties by input index (newer first)
		best := 0
		for i := 1; i < len(cursors); i++ {
			ak := cursors[best].entries[cursors[best].pos].Key
			bk := cursors[i].entries[cursors[i].pos].Key
			if bk < ak || (bk == ak && cursors[i].idx < cursors[best].idx) {
				best = i
			}
		}
		e := cursors[best].entries[cursors[best].pos]

		// advance or remove exhausted cursor
		cursors[best].pos++
		if cursors[best].pos >= len(cursors[best].entries) {
			cursors = append(cursors[:best], cursors[best+1:]...)
		}

		// skip older versions of the same key
		key := e.Key
		for len(cursors) > 0 {
			front := cursors[0].entries[cursors[0].pos]
			if front.Key != key {
				break
			}
			// find and advance all cursors pointing at the same key
			i := 0
			for i < len(cursors) {
				if cursors[i].entries[cursors[i].pos].Key == key {
					cursors[i].pos++
					if cursors[i].pos >= len(cursors[i].entries) {
						cursors = append(cursors[:i], cursors[i+1:]...)
					} else {
						i++
					}
				} else {
					i++
				}
			}
		}

		// elide tombstone at bottom level (no older data can exist below)
		if e.Deleted && bottomLevel {
			continue
		}
		merged = append(merged, e)
	}
	return merged
}

// CompactFiles performs a local compaction of the given SSTable files into a
// new SSTable at outPath.  It returns the metadata for the new file.
// The caller is responsible for swapping in the new file and deleting the
// inputs under the level lock.
func CompactFiles(inputs []*SSTableReader, outPath string, bottomLevel bool) (*SSTableMeta, error) {
	entrySlices := make([][]Entry, len(inputs))
	for i, r := range inputs {
		es, err := r.Iter()
		if err != nil {
			return nil, fmt.Errorf("iter sst %s: %w", r.Meta().Path, err)
		}
		// ensure sorted (they should be, but be defensive)
		sort.Slice(es, func(a, b int) bool { return es[a].Key < es[b].Key })
		entrySlices[i] = es
	}
	merged := MergeEntries(entrySlices, bottomLevel)

	w, err := NewSSTableWriter(outPath, len(merged)+1)
	if err != nil {
		return nil, err
	}
	for _, e := range merged {
		if err := w.Add(e); err != nil {
			return nil, err
		}
	}
	return w.Finish()
}

// CompactInMemory merges pre-loaded entry slices and writes a new SSTable.
// This is the path taken by the remote worker — it receives raw bytes and
// deserialises them rather than reading from local disk.
func CompactInMemory(inputs [][]Entry, outPath string, bottomLevel bool) (*SSTableMeta, error) {
	for i := range inputs {
		sort.Slice(inputs[i], func(a, b int) bool { return inputs[i][a].Key < inputs[i][b].Key })
	}
	merged := MergeEntries(inputs, bottomLevel)
	est := len(merged)
	if est < 1 {
		est = 1
	}
	w, err := NewSSTableWriter(outPath, est)
	if err != nil {
		return nil, err
	}
	for _, e := range merged {
		if err := w.Add(e); err != nil {
			return nil, err
		}
	}
	return w.Finish()
}
