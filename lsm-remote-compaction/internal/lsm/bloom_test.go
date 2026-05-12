package lsm

import (
	"fmt"
	"testing"
)

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)
	keys := make([]string, 500)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		bf.Add([]byte(keys[i]))
	}
	for _, k := range keys {
		if !bf.MayContain([]byte(k)) {
			t.Fatalf("false negative for key %q", k)
		}
	}
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	n := 1000
	bf := NewBloomFilter(n, 0.01)
	for i := range n {
		bf.Add([]byte(fmt.Sprintf("present-%d", i)))
	}
	fp := 0
	trials := 10_000
	for i := range trials {
		if bf.MayContain([]byte(fmt.Sprintf("absent-%d", i))) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	if rate > 0.05 { // allow 5× budget
		t.Fatalf("false positive rate %.3f exceeds threshold", rate)
	}
}

func TestBloomFilter_SerialiseRoundtrip(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	for i := range 50 {
		bf.Add([]byte(fmt.Sprintf("k%d", i)))
	}
	b := bf.Bytes()
	bf2 := BloomFilterFromBytes(b)
	for i := range 50 {
		k := []byte(fmt.Sprintf("k%d", i))
		if !bf2.MayContain(k) {
			t.Fatalf("round-tripped filter dropped key k%d", i)
		}
	}
}
