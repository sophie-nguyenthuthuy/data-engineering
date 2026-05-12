package lsm

import (
	"math/rand"
)

const maxLevel = 16
const probability = 0.25

type slNode struct {
	key     string
	value   []byte
	deleted bool          // tombstone
	next    []*slNode
}

// SkipList is a probabilistic sorted data structure used as the MemTable's
// backing store.  It provides O(log n) average-case Get/Put/Delete.
type SkipList struct {
	head  *slNode
	level int
	size  int // number of live entries
	rng   *rand.Rand
}

func newSkipList() *SkipList {
	head := &slNode{next: make([]*slNode, maxLevel)}
	return &SkipList{head: head, level: 1, rng: rand.New(rand.NewSource(42))}
}

func (sl *SkipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && sl.rng.Float64() < probability {
		lvl++
	}
	return lvl
}

// Put inserts or updates key with value.
func (sl *SkipList) Put(key string, value []byte) {
	update := make([]*slNode, maxLevel)
	cur := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && cur.next[i].key < key {
			cur = cur.next[i]
		}
		update[i] = cur
	}
	if cur.next[0] != nil && cur.next[0].key == key {
		n := cur.next[0]
		n.value = value
		n.deleted = false
		return
	}
	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}
	n := &slNode{key: key, value: value, next: make([]*slNode, lvl)}
	for i := range lvl {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	sl.size++
}

// Delete marks key as deleted (tombstone).  Returns false if key absent.
func (sl *SkipList) Delete(key string) bool {
	update := make([]*slNode, maxLevel)
	cur := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && cur.next[i].key < key {
			cur = cur.next[i]
		}
		update[i] = cur
	}
	target := cur.next[0]
	if target == nil || target.key != key {
		return false
	}
	target.deleted = true
	return true
}

// Get returns (value, found, tombstone).
func (sl *SkipList) Get(key string) ([]byte, bool, bool) {
	cur := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for cur.next[i] != nil && cur.next[i].key < key {
			cur = cur.next[i]
		}
	}
	n := cur.next[0]
	if n == nil || n.key != key {
		return nil, false, false
	}
	return n.value, true, n.deleted
}

// Iter returns all entries in sorted order (including tombstones).
func (sl *SkipList) Iter() []Entry {
	var out []Entry
	n := sl.head.next[0]
	for n != nil {
		out = append(out, Entry{Key: n.key, Value: n.value, Deleted: n.deleted})
		n = n.next[0]
	}
	return out
}

// Size returns the number of live (non-deleted) entries.
func (sl *SkipList) Size() int { return sl.size }
