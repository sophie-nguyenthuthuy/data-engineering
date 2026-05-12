package lsm

// SSTable (Sorted String Table) — immutable on-disk sorted key-value file.
//
// File layout:
//   [Data blocks ...]
//   [Index block]       — one entry per data block: (firstKey, offset, length)
//   [Bloom filter block]
//   [Footer: 40 bytes]
//     index_offset:8  index_len:8
//     bloom_offset:8  bloom_len:8
//     magic:8  (0xDEADBEEFCAFEBABE)
//
// Data block layout (variable length):
//   entry_count:4
//   for each entry:
//     type:1  key_len:4  key:key_len  val_len:4  val:val_len
//   crc32:4
//
// type: 0 = live value, 1 = tombstone

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

const (
	sstMagic         = uint64(0xDEADBEEFCAFEBABE)
	blockTargetBytes = 4 * 1024 // 4 KiB per data block
	bloomFPR         = 0.01
	entryLive        = byte(0)
	entryTombstone   = byte(1)
)

// SSTableMeta describes an SSTable on disk without loading it into memory.
type SSTableMeta struct {
	Path     string
	Level    int
	FirstKey string
	LastKey  string
	Size     int64 // bytes on disk
}

// ---- Writer ---------------------------------------------------------------

// SSTableWriter streams entries into an SSTable file.
type SSTableWriter struct {
	path    string
	f       *os.File
	bw      *bufio.Writer
	offset  int64

	// index
	indexEntries []indexEntry

	// current data block
	blockBuf    bytes.Buffer
	blockFirst  string
	blockCount  int
	blockOffset int64

	bloom   *BloomFilter
	estKeys int
}

type indexEntry struct {
	firstKey string
	offset   int64
	length   int32
}

// NewSSTableWriter opens path for writing and estimates estKeys entries for
// the bloom filter sizing.
func NewSSTableWriter(path string, estKeys int) (*SSTableWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create sstable %s: %w", path, err)
	}
	if estKeys < 1 {
		estKeys = 1
	}
	return &SSTableWriter{
		path:    path,
		f:       f,
		bw:      bufio.NewWriterSize(f, 1<<20),
		bloom:   NewBloomFilter(estKeys, bloomFPR),
		estKeys: estKeys,
	}, nil
}

// Add appends one entry.  Entries must arrive in sorted key order.
func (w *SSTableWriter) Add(e Entry) error {
	// Include tombstones in the bloom filter: callers need to find them to
	// propagate the deletion, not just skip them.
	w.bloom.Add([]byte(e.Key))
	typ := entryLive
	if e.Deleted {
		typ = entryTombstone
	}
	writeEntry(&w.blockBuf, typ, e.Key, e.Value)
	if w.blockFirst == "" {
		w.blockFirst = e.Key
	}
	w.blockCount++
	if w.blockBuf.Len() >= blockTargetBytes {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}
	return nil
}

// Finish flushes any remaining data, writes the index, bloom filter, and footer.
func (w *SSTableWriter) Finish() (*SSTableMeta, error) {
	if w.blockCount > 0 {
		if err := w.flushBlock(); err != nil {
			return nil, err
		}
	}
	indexOffset := w.offset
	indexLen, err := w.writeIndex()
	if err != nil {
		return nil, err
	}
	bloomOffset := w.offset
	bloomBytes := w.bloom.Bytes()
	if err := w.writeRaw(bloomBytes); err != nil {
		return nil, err
	}
	bloomLen := int64(len(bloomBytes))

	// footer
	var foot [40]byte
	binary.LittleEndian.PutUint64(foot[0:], uint64(indexOffset))
	binary.LittleEndian.PutUint64(foot[8:], uint64(indexLen))
	binary.LittleEndian.PutUint64(foot[16:], uint64(bloomOffset))
	binary.LittleEndian.PutUint64(foot[24:], uint64(bloomLen))
	binary.LittleEndian.PutUint64(foot[32:], sstMagic)
	if err := w.writeRaw(foot[:]); err != nil {
		return nil, err
	}
	if err := w.bw.Flush(); err != nil {
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	size := w.offset
	w.f.Close()

	first, last := "", ""
	if len(w.indexEntries) > 0 {
		first = w.indexEntries[0].firstKey
		// last key: would need to store it; use firstKey of last block as approx
		last = w.indexEntries[len(w.indexEntries)-1].firstKey
	}
	return &SSTableMeta{
		Path:     w.path,
		FirstKey: first,
		LastKey:  last,
		Size:     size,
	}, nil
}

func (w *SSTableWriter) flushBlock() error {
	var full bytes.Buffer
	binary.Write(&full, binary.LittleEndian, uint32(w.blockCount))
	full.Write(w.blockBuf.Bytes())
	// CRC over header + body
	crc := crc32.ChecksumIEEE(full.Bytes())
	var cs [4]byte
	binary.LittleEndian.PutUint32(cs[:], crc)
	full.Write(cs[:])

	start := w.offset
	data := full.Bytes()
	w.indexEntries = append(w.indexEntries, indexEntry{
		firstKey: w.blockFirst,
		offset:   start,
		length:   int32(len(data)),
	})
	if err := w.writeRaw(data); err != nil {
		return err
	}
	w.blockBuf.Reset()
	w.blockFirst = ""
	w.blockCount = 0
	return nil
}

func (w *SSTableWriter) writeIndex() (int64, error) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(len(w.indexEntries)))
	for _, ie := range w.indexEntries {
		binary.Write(&buf, binary.LittleEndian, uint32(len(ie.firstKey)))
		buf.WriteString(ie.firstKey)
		binary.Write(&buf, binary.LittleEndian, uint64(ie.offset))
		binary.Write(&buf, binary.LittleEndian, uint32(ie.length))
	}
	n := int64(buf.Len())
	return n, w.writeRaw(buf.Bytes())
}

func (w *SSTableWriter) writeRaw(data []byte) error {
	_, err := w.bw.Write(data)
	if err == nil {
		w.offset += int64(len(data))
	}
	return err
}

func writeEntry(buf *bytes.Buffer, typ byte, key string, value []byte) {
	buf.WriteByte(typ)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(key)))
	binary.LittleEndian.PutUint32(hdr[4:], uint32(len(value)))
	buf.Write(hdr[:])
	buf.WriteString(key)
	buf.Write(value)
}

// ---- Reader ---------------------------------------------------------------

// SSTableReader provides point lookups and full iteration over an SSTable.
type SSTableReader struct {
	f     *os.File
	index []indexEntry
	bloom *BloomFilter
	meta  SSTableMeta
}

// OpenSSTable opens an existing SSTable for reading.
func OpenSSTable(path string, level int) (*SSTableReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sstable %s: %w", path, err)
	}
	r := &SSTableReader{f: f, meta: SSTableMeta{Path: path, Level: level}}
	if err := r.loadFooter(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

func (r *SSTableReader) loadFooter() error {
	fi, err := r.f.Stat()
	if err != nil {
		return err
	}
	size := fi.Size()
	if size < 40 {
		return fmt.Errorf("sstable too small")
	}
	var foot [40]byte
	if _, err := r.f.ReadAt(foot[:], size-40); err != nil {
		return err
	}
	if binary.LittleEndian.Uint64(foot[32:]) != sstMagic {
		return fmt.Errorf("bad sstable magic")
	}
	indexOffset := int64(binary.LittleEndian.Uint64(foot[0:]))
	indexLen := int64(binary.LittleEndian.Uint64(foot[8:]))
	bloomOffset := int64(binary.LittleEndian.Uint64(foot[16:]))
	bloomLen := int64(binary.LittleEndian.Uint64(foot[24:]))

	// bloom
	bloomData := make([]byte, bloomLen)
	if _, err := r.f.ReadAt(bloomData, bloomOffset); err != nil {
		return err
	}
	r.bloom = BloomFilterFromBytes(bloomData)

	// index
	indexData := make([]byte, indexLen)
	if _, err := r.f.ReadAt(indexData, indexOffset); err != nil {
		return err
	}
	r.index = parseIndex(indexData)

	r.meta.Size = size
	if len(r.index) > 0 {
		r.meta.FirstKey = r.index[0].firstKey
		r.meta.LastKey = r.index[len(r.index)-1].firstKey
	}
	return nil
}

func parseIndex(data []byte) []indexEntry {
	if len(data) < 4 {
		return nil
	}
	count := binary.LittleEndian.Uint32(data[:4])
	data = data[4:]
	out := make([]indexEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		kl := binary.LittleEndian.Uint32(data[:4])
		data = data[4:]
		key := string(data[:kl])
		data = data[kl:]
		offset := int64(binary.LittleEndian.Uint64(data[:8]))
		data = data[8:]
		length := int32(binary.LittleEndian.Uint32(data[:4]))
		data = data[4:]
		out = append(out, indexEntry{firstKey: key, offset: offset, length: length})
	}
	return out
}

// Get looks up key.  Returns (value, found, tombstone).
func (r *SSTableReader) Get(key string) ([]byte, bool, bool) {
	if !r.bloom.MayContain([]byte(key)) {
		return nil, false, false
	}
	blockIdx := r.blockFor(key)
	if blockIdx < 0 {
		return nil, false, false
	}
	entries, err := r.readBlock(blockIdx)
	if err != nil {
		return nil, false, false
	}
	idx := sort.Search(len(entries), func(i int) bool { return entries[i].Key >= key })
	if idx < len(entries) && entries[idx].Key == key {
		e := entries[idx]
		return e.Value, true, e.Deleted
	}
	return nil, false, false
}

// blockFor returns the index of the data block that could contain key.
func (r *SSTableReader) blockFor(key string) int {
	if len(r.index) == 0 {
		return -1
	}
	// last block whose firstKey <= key
	hi := sort.Search(len(r.index), func(i int) bool { return r.index[i].firstKey > key })
	hi--
	if hi < 0 {
		return -1
	}
	return hi
}

// Iter returns all entries in key order for compaction.
func (r *SSTableReader) Iter() ([]Entry, error) {
	var out []Entry
	for i := range r.index {
		entries, err := r.readBlock(i)
		if err != nil {
			return nil, err
		}
		out = append(out, entries...)
	}
	return out, nil
}

func (r *SSTableReader) readBlock(idx int) ([]Entry, error) {
	ie := r.index[idx]
	data := make([]byte, ie.length)
	if _, err := r.f.ReadAt(data, ie.offset); err != nil {
		return nil, err
	}
	if len(data) < 8 {
		return nil, io.ErrUnexpectedEOF
	}
	// verify CRC
	body := data[:len(data)-4]
	gotCRC := crc32.ChecksumIEEE(body)
	wantCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	if gotCRC != wantCRC {
		return nil, fmt.Errorf("block %d CRC mismatch", idx)
	}
	count := binary.LittleEndian.Uint32(body[:4])
	body = body[4:]
	entries := make([]Entry, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(body) < 9 {
			return nil, io.ErrUnexpectedEOF
		}
		typ := body[0]
		kl := binary.LittleEndian.Uint32(body[1:5])
		vl := binary.LittleEndian.Uint32(body[5:9])
		body = body[9:]
		key := string(body[:kl])
		body = body[kl:]
		val := body[:vl]
		body = body[vl:]
		entries = append(entries, Entry{Key: key, Value: val, Deleted: typ == entryTombstone})
	}
	return entries, nil
}

// Meta returns lightweight metadata without re-reading the file.
func (r *SSTableReader) Meta() SSTableMeta { return r.meta }

// Close releases the file descriptor.
func (r *SSTableReader) Close() error { return r.f.Close() }

// Bytes reads the entire SSTable into memory (used for shipping over gRPC).
func (r *SSTableReader) Bytes() ([]byte, error) {
	r.f.Seek(0, io.SeekStart)
	return io.ReadAll(r.f)
}
