package lsm

import (
	"encoding/binary"
	"math"
)

// BloomFilter is a space-efficient probabilistic set membership structure.
// False positives are possible; false negatives are not.
// Uses k independent hash functions derived from two base hashes (Kirsch-Mitzenmacher).
type BloomFilter struct {
	bits []uint64
	k    uint   // number of hash functions
	m    uint   // total number of bits
}

// NewBloomFilter creates a filter sized for n expected elements at fpr false-positive rate.
func NewBloomFilter(n int, fpr float64) *BloomFilter {
	m := optimalM(n, fpr)
	k := optimalK(m, uint(n))
	return &BloomFilter{
		bits: make([]uint64, (m+63)/64),
		k:    k,
		m:    m,
	}
}

func optimalM(n int, fpr float64) uint {
	// m = -n*ln(fpr) / (ln2)^2
	return uint(math.Ceil(-float64(n) * math.Log(fpr) / (math.Ln2 * math.Ln2)))
}

func optimalK(m, n uint) uint {
	// k = (m/n) * ln2
	return uint(math.Round(float64(m) / float64(n) * math.Ln2))
}

// Add inserts key into the filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := hash128(key)
	for i := uint(0); i < bf.k; i++ {
		bit := (h1 + uint64(i)*h2) % uint64(bf.m)
		bf.bits[bit/64] |= 1 << (bit % 64)
	}
}

// MayContain returns false only if key is definitely absent.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := hash128(key)
	for i := uint(0); i < bf.k; i++ {
		bit := (h1 + uint64(i)*h2) % uint64(bf.m)
		if bf.bits[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// Bytes serialises the filter for storage in an SSTable footer.
func (bf *BloomFilter) Bytes() []byte {
	buf := make([]byte, 8+8+len(bf.bits)*8)
	binary.LittleEndian.PutUint64(buf[0:], uint64(bf.k))
	binary.LittleEndian.PutUint64(buf[8:], uint64(bf.m))
	for i, w := range bf.bits {
		binary.LittleEndian.PutUint64(buf[16+i*8:], w)
	}
	return buf
}

// BloomFilterFromBytes deserialises a filter stored by Bytes.
func BloomFilterFromBytes(b []byte) *BloomFilter {
	k := binary.LittleEndian.Uint64(b[0:])
	m := binary.LittleEndian.Uint64(b[8:])
	words := (len(b) - 16) / 8
	bits := make([]uint64, words)
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(b[16+i*8:])
	}
	return &BloomFilter{bits: bits, k: uint(k), m: uint(m)}
}

// hash128 returns two independent 64-bit hashes using FNV-1a + xorshift.
// Kirsch-Mitzenmacher uses h1 + i*h2 to derive k independent positions.
func hash128(data []byte) (uint64, uint64) {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h1 := uint64(offset64)
	for _, b := range data {
		h1 ^= uint64(b)
		h1 *= prime64
	}
	// derive h2 via xorshift
	h2 := h1 ^ (h1 >> 33)
	h2 *= 0xff51afd7ed558ccd
	h2 ^= h2 >> 33
	h2 *= 0xc4ceb9fe1a85ec53
	h2 ^= h2 >> 33
	return h1, h2
}
