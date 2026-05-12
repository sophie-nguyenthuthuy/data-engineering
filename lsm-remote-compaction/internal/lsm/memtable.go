package lsm

import "sync"

// Entry is a key-value record.  Deleted=true signals a tombstone.
type Entry struct {
	Key     string
	Value   []byte
	Deleted bool
	SeqNum  uint64 // monotonically increasing write sequence number
}

// MemTable is a concurrent, size-bounded in-memory sorted table backed by a
// skip list.  When SizeBytes exceeds the configured threshold, the LSM tree
// freezes it and flushes it to an L0 SSTable.
type MemTable struct {
	mu       sync.RWMutex
	sl       *SkipList
	sizeByt  int64
	seq      uint64
}

func newMemTable() *MemTable {
	return &MemTable{sl: newSkipList()}
}

// Put inserts or overwrites key.
func (m *MemTable) Put(key string, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.sl.Put(key, value)
	m.sizeByt += int64(len(key) + len(value))
}

// Delete records a tombstone for key.
func (m *MemTable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seq++
	m.sl.Delete(key)
	m.sizeByt += int64(len(key))
}

// Get looks up key.  Returns (value, found, tombstone).
func (m *MemTable) Get(key string) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Get(key)
}

// Entries returns a sorted snapshot suitable for SSTable flushing.
func (m *MemTable) Entries() []Entry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sl.Iter()
}

// SizeBytes is the approximate in-memory footprint of live data.
func (m *MemTable) SizeBytes() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeByt
}
