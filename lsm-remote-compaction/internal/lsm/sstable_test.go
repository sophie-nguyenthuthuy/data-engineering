package lsm

import (
	"fmt"
	"os"
	"testing"
)

func TestSSTable_WriteRead(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "*.sst")
	f.Close()
	path := f.Name()

	w, err := NewSSTableWriter(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]byte{
		"apple":  []byte("red"),
		"banana": []byte("yellow"),
		"cherry": []byte("dark-red"),
	}
	// sorted order
	for _, k := range []string{"apple", "banana", "cherry"} {
		if err := w.Add(Entry{Key: k, Value: want[k]}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}

	r, err := OpenSSTable(path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for k, v := range want {
		got, found, del := r.Get(k)
		if !found || del {
			t.Fatalf("Get(%q): found=%v del=%v", k, found, del)
		}
		if string(got) != string(v) {
			t.Fatalf("Get(%q) = %q, want %q", k, got, v)
		}
	}
	_, found, _ := r.Get("notexist")
	if found {
		t.Fatal("expected miss for absent key")
	}
}

func TestSSTable_Tombstone(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/t.sst"
	w, _ := NewSSTableWriter(path, 10)
	_ = w.Add(Entry{Key: "gone", Deleted: true})
	_ = w.Add(Entry{Key: "here", Value: []byte("yes")})
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, _ := OpenSSTable(path, 0)
	defer r.Close()

	_, found, del := r.Get("gone")
	if !found || !del {
		t.Fatalf("tombstone: found=%v del=%v", found, del)
	}
}

func TestSSTable_Iter(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/i.sst"
	w, _ := NewSSTableWriter(path, 200)
	for i := range 200 {
		_ = w.Add(Entry{Key: fmt.Sprintf("key-%05d", i), Value: []byte("v")})
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, _ := OpenSSTable(path, 0)
	defer r.Close()
	entries, err := r.Iter()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 200 {
		t.Fatalf("got %d entries, want 200", len(entries))
	}
	for i, e := range entries {
		want := fmt.Sprintf("key-%05d", i)
		if e.Key != want {
			t.Fatalf("entries[%d].Key = %q, want %q", i, e.Key, want)
		}
	}
}
