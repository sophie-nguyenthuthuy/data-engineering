package lsm

import (
	"fmt"
	"testing"
)

func openTestTree(t *testing.T) *Tree {
	t.Helper()
	cfg := Config{
		Dir:                   t.TempDir(),
		MemTableSizeBytes:     64 * 1024, // 64 KiB — small to trigger flushes
		L0CompactionThreshold: 2,
		L1MaxBytes:            256 * 1024,
	}
	tree, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tree.Close() })
	return tree
}

func TestTree_PutGet(t *testing.T) {
	tree := openTestTree(t)
	if err := tree.Put("hello", []byte("world")); err != nil {
		t.Fatal(err)
	}
	v, ok := tree.Get("hello")
	if !ok {
		t.Fatal("expected to find key")
	}
	if string(v) != "world" {
		t.Fatalf("got %q, want %q", v, "world")
	}
}

func TestTree_Delete(t *testing.T) {
	tree := openTestTree(t)
	_ = tree.Put("toDelete", []byte("v"))
	_ = tree.Delete("toDelete")
	_, ok := tree.Get("toDelete")
	if ok {
		t.Fatal("deleted key should not be found")
	}
}

func TestTree_Overwrite(t *testing.T) {
	tree := openTestTree(t)
	_ = tree.Put("k", []byte("v1"))
	_ = tree.Put("k", []byte("v2"))
	v, _ := tree.Get("k")
	if string(v) != "v2" {
		t.Fatalf("want v2, got %q", v)
	}
}

func TestTree_ManyKeys(t *testing.T) {
	tree := openTestTree(t)
	const n = 2000
	for i := range n {
		if err := tree.Put(fmt.Sprintf("key-%05d", i), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	for i := range n {
		k := fmt.Sprintf("key-%05d", i)
		v, ok := tree.Get(k)
		if !ok {
			t.Fatalf("missing key %s", k)
		}
		want := fmt.Sprintf("val-%d", i)
		if string(v) != want {
			t.Fatalf("key %s: got %q, want %q", k, v, want)
		}
	}
}

func TestTree_CompactionPreservesData(t *testing.T) {
	tree := openTestTree(t)
	// Write enough data to trigger multiple L0 flushes and a compaction
	for i := range 5000 {
		_ = tree.Put(fmt.Sprintf("ck-%06d", i), []byte(fmt.Sprintf("value-%d", i)))
	}
	// Force close + reopen to ensure everything is on disk
	tree.Close()

	cfg := Config{
		Dir:                   tree.cfg.Dir,
		MemTableSizeBytes:     64 * 1024,
		L0CompactionThreshold: 2,
	}
	tree2, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer tree2.Close()

	// spot-check a few keys
	for _, i := range []int{0, 100, 999, 2500, 4999} {
		k := fmt.Sprintf("ck-%06d", i)
		v, ok := tree2.Get(k)
		if !ok {
			t.Errorf("key %s missing after reopen", k)
			continue
		}
		want := fmt.Sprintf("value-%d", i)
		if string(v) != want {
			t.Errorf("key %s: got %q, want %q", k, v, want)
		}
	}
}

func TestCompactionMerge_TombstoneElision(t *testing.T) {
	older := []Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}
	newer := []Entry{
		{Key: "a", Deleted: true},
	}
	// Bottom level: tombstone should be elided
	merged := MergeEntries([][]Entry{newer, older}, true)
	for _, e := range merged {
		if e.Key == "a" {
			t.Fatal("tombstone not elided at bottom level")
		}
	}
	// Non-bottom: tombstone must survive
	merged = MergeEntries([][]Entry{newer, older}, false)
	found := false
	for _, e := range merged {
		if e.Key == "a" && e.Deleted {
			found = true
		}
	}
	if !found {
		t.Fatal("tombstone elided at non-bottom level")
	}
}
