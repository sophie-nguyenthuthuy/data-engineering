package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL (Write-Ahead Log) records every mutation before it touches the
// MemTable.  On crash recovery the log is replayed in order to reconstruct
// the in-flight MemTable.
//
// Record format (all little-endian):
//   [type:1][key_len:4][key:key_len][val_len:4][val:val_len][crc32:4]
// type: 0 = Put, 1 = Delete

const (
	walPut    byte = 0
	walDelete byte = 1
)

// WAL is a file-backed write-ahead log.
type WAL struct {
	f   *os.File
	bw  *bufio.Writer
	seq uint64
}

// OpenWAL opens (or creates) a WAL at path.
func OpenWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal %s: %w", path, err)
	}
	return &WAL{f: f, bw: bufio.NewWriterSize(f, 64*1024)}, nil
}

// AppendPut records a Put before it is applied to the MemTable.
func (w *WAL) AppendPut(key string, value []byte) error {
	return w.appendRecord(walPut, []byte(key), value)
}

// AppendDelete records a Delete before it is applied to the MemTable.
func (w *WAL) AppendDelete(key string) error {
	return w.appendRecord(walDelete, []byte(key), nil)
}

// Sync flushes the write buffer and syncs to disk.
func (w *WAL) Sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Close flushes, syncs, and closes the underlying file.
func (w *WAL) Close() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// Delete removes the WAL file after a successful MemTable flush.
func (w *WAL) Delete() error {
	name := w.f.Name()
	_ = w.f.Close()
	return os.Remove(name)
}

func (w *WAL) appendRecord(typ byte, key, value []byte) error {
	var buf [9]byte
	buf[0] = typ
	binary.LittleEndian.PutUint32(buf[1:5], uint32(len(key)))
	binary.LittleEndian.PutUint32(buf[5:9], uint32(len(value)))

	crc := crc32.NewIEEE()
	crc.Write(buf[:9])
	crc.Write(key)
	crc.Write(value)

	if _, err := w.bw.Write(buf[:]); err != nil {
		return err
	}
	if _, err := w.bw.Write(key); err != nil {
		return err
	}
	if len(value) > 0 {
		if _, err := w.bw.Write(value); err != nil {
			return err
		}
	}
	var cs [4]byte
	binary.LittleEndian.PutUint32(cs[:], crc.Sum32())
	_, err := w.bw.Write(cs[:])
	return err
}

// Replay reads an existing WAL and calls put/del for each valid record.
// Corrupted (CRC-mismatched) records stop replay — data after a corruption
// is considered lost.
func ReplayWAL(path string, put func(key string, val []byte), del func(key string)) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	br := bufio.NewReader(f)
	for {
		var hdr [9]byte
		if _, err := io.ReadFull(br, hdr[:]); err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		} else if err != nil {
			return err
		}
		typ := hdr[0]
		klen := binary.LittleEndian.Uint32(hdr[1:5])
		vlen := binary.LittleEndian.Uint32(hdr[5:9])

		key := make([]byte, klen)
		val := make([]byte, vlen)
		if _, err := io.ReadFull(br, key); err != nil {
			return err
		}
		if vlen > 0 {
			if _, err := io.ReadFull(br, val); err != nil {
				return err
			}
		}
		var csBytes [4]byte
		if _, err := io.ReadFull(br, csBytes[:]); err != nil {
			return err
		}
		crc := crc32.NewIEEE()
		crc.Write(hdr[:])
		crc.Write(key)
		crc.Write(val)
		if crc.Sum32() != binary.LittleEndian.Uint32(csBytes[:]) {
			return fmt.Errorf("WAL CRC mismatch at key=%q — log truncated here", key)
		}
		switch typ {
		case walPut:
			put(string(key), val)
		case walDelete:
			del(string(key))
		}
	}
}
