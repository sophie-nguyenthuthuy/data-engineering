// Package lsm implements a Log-Structured Merge Tree with tiered remote
// compaction.  The local node serves reads and writes at all times; compaction
// work is shipped to a remote worker over gRPC and finalised through a
// quorum-based commit protocol.
package lsm

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Config controls the LSM engine behaviour.
type Config struct {
	// Dir is the directory where SSTables and the WAL are stored.
	Dir string

	// MemTableSizeBytes triggers a flush when the active MemTable exceeds it.
	MemTableSizeBytes int64

	// L0CompactionThreshold is the number of L0 files that triggers a compaction.
	L0CompactionThreshold int

	// LevelSizeMultiplier determines how much larger each level is vs. the previous.
	LevelSizeMultiplier int64

	// L1MaxBytes is the byte budget for L1; higher levels are multiplied.
	L1MaxBytes int64

	// NumLevels is the total number of levels (L0 through L{NumLevels-1}).
	NumLevels int

	// Compactor is called when a compaction is needed.  If nil, local compaction
	// is used.  Inject a RemoteCompactor here to enable remote compaction.
	Compactor CompactionHandler

	Logger *slog.Logger
}

func (c *Config) setDefaults() {
	if c.MemTableSizeBytes == 0 {
		c.MemTableSizeBytes = 4 * 1024 * 1024 // 4 MiB
	}
	if c.L0CompactionThreshold == 0 {
		c.L0CompactionThreshold = 4
	}
	if c.LevelSizeMultiplier == 0 {
		c.LevelSizeMultiplier = 10
	}
	if c.L1MaxBytes == 0 {
		c.L1MaxBytes = 10 * 1024 * 1024 // 10 MiB
	}
	if c.NumLevels == 0 {
		c.NumLevels = 7
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// CompactionHandler is called with a set of files to compact.
// Implementors may perform the compaction locally or remotely.
type CompactionHandler interface {
	Compact(ctx context.Context, inputs []*SSTableReader, outPath string, targetLevel int, bottomLevel bool) (*SSTableMeta, error)
}

// LocalCompactor performs compaction on the local node.
type LocalCompactor struct{}

func (LocalCompactor) Compact(_ context.Context, inputs []*SSTableReader, outPath string, _ int, bottomLevel bool) (*SSTableMeta, error) {
	return CompactFiles(inputs, outPath, bottomLevel)
}

// ---- level ---------------------------------------------------------------

type level struct {
	mu    sync.RWMutex
	files []*SSTableReader // sorted by FirstKey for L1+; unordered for L0
}

func (lv *level) add(r *SSTableReader) {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	lv.files = append(lv.files, r)
}

func (lv *level) remove(paths map[string]struct{}) {
	lv.mu.Lock()
	defer lv.mu.Unlock()
	kept := lv.files[:0]
	for _, f := range lv.files {
		if _, del := paths[f.Meta().Path]; !del {
			kept = append(kept, f)
		}
	}
	lv.files = kept
}

func (lv *level) totalBytes() int64 {
	lv.mu.RLock()
	defer lv.mu.RUnlock()
	var n int64
	for _, f := range lv.files {
		n += f.Meta().Size
	}
	return n
}

func (lv *level) count() int {
	lv.mu.RLock()
	defer lv.mu.RUnlock()
	return len(lv.files)
}

// ---- Tree ----------------------------------------------------------------

// Tree is the main LSM engine.
type Tree struct {
	cfg    Config
	log    *slog.Logger
	seqGen atomic.Uint64

	// write path
	walMu  sync.Mutex
	wal    *WAL
	mu     sync.RWMutex // protects mem + immutable
	mem    *MemTable    // active memtable
	imm    []*MemTable  // immutable memtables awaiting flush

	// level 0..N-1
	levels []*level

	// background flush/compact
	flushCh   chan struct{}
	compactCh chan struct{}
	wg        sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
}

// Open initialises or recovers a Tree in cfg.Dir.
func Open(cfg Config) (*Tree, error) {
	cfg.setDefaults()
	if cfg.Compactor == nil {
		cfg.Compactor = LocalCompactor{}
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	t := &Tree{
		cfg:       cfg,
		log:       cfg.Logger,
		levels:    make([]*level, cfg.NumLevels),
		flushCh:   make(chan struct{}, 4),
		compactCh: make(chan struct{}, 4),
		closeCh:   make(chan struct{}),
	}
	for i := range t.levels {
		t.levels[i] = &level{}
	}

	// recover WAL + open existing SSTables
	if err := t.recover(); err != nil {
		return nil, err
	}

	t.wg.Add(2)
	go t.flushLoop()
	go t.compactLoop()
	return t, nil
}

func (t *Tree) recover() error {
	t.mem = newMemTable()
	walPath := filepath.Join(t.cfg.Dir, "wal.log")
	// replay existing WAL into mem
	if err := ReplayWAL(walPath, t.mem.Put, t.mem.Delete); err != nil {
		t.log.Warn("WAL replay stopped", "err", err)
	}
	// open existing SSTable files
	entries, err := os.ReadDir(t.cfg.Dir)
	if err != nil {
		return err
	}
	for _, de := range entries {
		if filepath.Ext(de.Name()) != ".sst" {
			continue
		}
		path := filepath.Join(t.cfg.Dir, de.Name())
		level := levelFromPath(de.Name())
		r, err := OpenSSTable(path, level)
		if err != nil {
			t.log.Warn("skip corrupt SSTable", "path", path, "err", err)
			continue
		}
		t.levels[level].add(r)
	}
	// re-open WAL for appending
	wal, err := OpenWAL(walPath)
	if err != nil {
		return err
	}
	t.wal = wal
	return nil
}

// Put writes a key-value pair.
func (t *Tree) Put(key string, value []byte) error {
	t.walMu.Lock()
	if err := t.wal.AppendPut(key, value); err != nil {
		t.walMu.Unlock()
		return fmt.Errorf("wal put: %w", err)
	}
	if err := t.wal.Sync(); err != nil {
		t.walMu.Unlock()
		return fmt.Errorf("wal sync: %w", err)
	}
	t.walMu.Unlock()

	t.mu.Lock()
	t.mem.Put(key, value)
	needFlush := t.mem.SizeBytes() >= t.cfg.MemTableSizeBytes
	t.mu.Unlock()

	if needFlush {
		select {
		case t.flushCh <- struct{}{}:
		default:
		}
	}
	return nil
}

// Delete records a tombstone for key.
func (t *Tree) Delete(key string) error {
	t.walMu.Lock()
	if err := t.wal.AppendDelete(key); err != nil {
		t.walMu.Unlock()
		return err
	}
	_ = t.wal.Sync()
	t.walMu.Unlock()

	t.mu.Lock()
	t.mem.Delete(key)
	t.mu.Unlock()
	return nil
}

// Get reads the latest value for key.  Returns (nil, false) if not found.
func (t *Tree) Get(key string) ([]byte, bool) {
	// 1. active memtable
	t.mu.RLock()
	v, found, deleted := t.mem.Get(key)
	imm := t.imm
	t.mu.RUnlock()
	if found {
		if deleted {
			return nil, false
		}
		return v, true
	}

	// 2. immutable memtables (newest first)
	for i := len(imm) - 1; i >= 0; i-- {
		v, found, deleted = imm[i].Get(key)
		if found {
			if deleted {
				return nil, false
			}
			return v, true
		}
	}

	// 3. levels (L0 → LN)
	// L0 may have overlapping key ranges; check every file
	t.levels[0].mu.RLock()
	l0 := make([]*SSTableReader, len(t.levels[0].files))
	copy(l0, t.levels[0].files)
	t.levels[0].mu.RUnlock()
	for i := len(l0) - 1; i >= 0; i-- { // newest first
		v, found, deleted = l0[i].Get(key)
		if found {
			if deleted {
				return nil, false
			}
			return v, true
		}
	}

	// L1+ files are non-overlapping; binary search within each level
	for lvl := 1; lvl < len(t.levels); lvl++ {
		t.levels[lvl].mu.RLock()
		files := make([]*SSTableReader, len(t.levels[lvl].files))
		copy(files, t.levels[lvl].files)
		t.levels[lvl].mu.RUnlock()

		// find candidate file by key range
		idx := sort.Search(len(files), func(i int) bool {
			return files[i].Meta().LastKey >= key
		})
		if idx < len(files) {
			v, found, deleted = files[idx].Get(key)
			if found {
				if deleted {
					return nil, false
				}
				return v, true
			}
		}
	}
	return nil, false
}

// Close flushes pending data and shuts down background goroutines.
// Safe to call multiple times.
func (t *Tree) Close() error {
	var err error
	t.closeOnce.Do(func() {
		close(t.closeCh)
		t.wg.Wait()

		t.mu.Lock()
		if t.mem.SizeBytes() > 0 {
			t.imm = append(t.imm, t.mem)
			t.mem = newMemTable()
		}
		t.mu.Unlock()
		for _, m := range t.imm {
			if ferr := t.flushMemTable(m); ferr != nil {
				t.log.Error("flush on close", "err", ferr)
			}
		}
		err = t.wal.Close()
	})
	return err
}

// ---- background workers --------------------------------------------------

func (t *Tree) flushLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-t.closeCh:
			return
		case <-t.flushCh:
		case <-ticker.C:
		}
		t.maybeFlush()
	}
}

func (t *Tree) compactLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-t.closeCh:
			return
		case <-t.compactCh:
		case <-ticker.C:
		}
		t.maybeCompact()
	}
}

func (t *Tree) maybeFlush() {
	t.mu.Lock()
	if t.mem.SizeBytes() < t.cfg.MemTableSizeBytes && len(t.imm) == 0 {
		t.mu.Unlock()
		return
	}
	if t.mem.SizeBytes() >= t.cfg.MemTableSizeBytes {
		t.imm = append(t.imm, t.mem)
		t.mem = newMemTable()
	}
	toFlush := t.imm[0]
	t.mu.Unlock()

	if err := t.flushMemTable(toFlush); err != nil {
		t.log.Error("flush failed", "err", err)
		return
	}
	t.mu.Lock()
	t.imm = t.imm[1:]
	t.mu.Unlock()

	// rotate WAL after flush
	t.walMu.Lock()
	oldWAL := t.wal
	newWAL, err := OpenWAL(filepath.Join(t.cfg.Dir, "wal.log"))
	if err == nil {
		t.wal = newWAL
	}
	t.walMu.Unlock()
	_ = oldWAL.Delete()

	select {
	case t.compactCh <- struct{}{}:
	default:
	}
}

func (t *Tree) flushMemTable(m *MemTable) error {
	entries := m.Entries()
	if len(entries) == 0 {
		return nil
	}
	path := t.newSSTPath(0)
	w, err := NewSSTableWriter(path, len(entries))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			return err
		}
	}
	meta, err := w.Finish()
	if err != nil {
		return err
	}
	meta.Level = 0
	r, err := OpenSSTable(meta.Path, 0)
	if err != nil {
		return err
	}
	t.levels[0].add(r)
	t.log.Info("flushed L0 SSTable", "path", meta.Path, "bytes", meta.Size)
	return nil
}

func (t *Tree) maybeCompact() {
	// L0 threshold check
	if t.levels[0].count() >= t.cfg.L0CompactionThreshold {
		if err := t.compactLevel(0); err != nil {
			t.log.Error("L0 compaction", "err", err)
		}
	}
	// size threshold for L1..LN-2
	for lvl := 1; lvl < t.cfg.NumLevels-1; lvl++ {
		maxBytes := t.cfg.L1MaxBytes
		for i := 0; i < lvl-1; i++ {
			maxBytes *= t.cfg.LevelSizeMultiplier
		}
		if t.levels[lvl].totalBytes() > maxBytes {
			if err := t.compactLevel(lvl); err != nil {
				t.log.Error("level compaction", "level", lvl, "err", err)
			}
		}
	}
}

func (t *Tree) compactLevel(lvl int) error {
	t.levels[lvl].mu.RLock()
	inputs := make([]*SSTableReader, len(t.levels[lvl].files))
	copy(inputs, t.levels[lvl].files)
	t.levels[lvl].mu.RUnlock()

	if len(inputs) == 0 {
		return nil
	}

	targetLevel := lvl + 1
	if targetLevel >= t.cfg.NumLevels {
		targetLevel = t.cfg.NumLevels - 1
	}
	outPath := t.newSSTPath(targetLevel)
	bottom := targetLevel == t.cfg.NumLevels-1

	t.log.Info("starting compaction", "from_level", lvl, "to_level", targetLevel, "files", len(inputs))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	meta, err := t.cfg.Compactor.Compact(ctx, inputs, outPath, targetLevel, bottom)
	if err != nil {
		return fmt.Errorf("compact L%d→L%d: %w", lvl, targetLevel, err)
	}
	meta.Level = targetLevel

	r, err := OpenSSTable(meta.Path, targetLevel)
	if err != nil {
		return err
	}

	// atomically swap: add new file, remove inputs
	removed := make(map[string]struct{}, len(inputs))
	for _, in := range inputs {
		removed[in.Meta().Path] = struct{}{}
	}
	t.levels[targetLevel].add(r)
	t.levels[lvl].remove(removed)

	// close and delete old files
	for _, in := range inputs {
		_ = in.Close()
		_ = os.Remove(in.Meta().Path)
	}
	t.log.Info("compaction done", "output", meta.Path, "bytes", meta.Size)
	return nil
}

// ---- helpers -------------------------------------------------------------

func (t *Tree) newSSTPath(lvl int) string {
	seq := t.seqGen.Add(1)
	name := fmt.Sprintf("L%d_%020d.sst", lvl, seq)
	return filepath.Join(t.cfg.Dir, name)
}

func levelFromPath(name string) int {
	var lvl int
	fmt.Sscanf(name, "L%d_", &lvl)
	return lvl
}
