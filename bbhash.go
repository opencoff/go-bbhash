// bbhash.go - fast minimal perfect hashing for massive key sets
//
// Implements the BBHash algorithm in: https://arxiv.org/abs/1702.03154
//
// Inspired by D Gryski's implementation of BBHash (https://github.com/dgryski/go-boomphf)
//
// (c) Sudhi Herle 2018
//
// License GPLv2

// Package bbhash implements BBHash - a new algorithm for creating fast, minimal perfect hash
// functions as described in: https://arxiv.org/abs/1702.03154
package bbhash

import (
	"fmt"

	"crypto/rand"
	"encoding/binary"
)

// BBHash represents a computed minimal perfect hash for a given set of keys.
type BBHash struct {
	bits  []*BitVector
	ranks []uint64
	salt  uint64

	// Mapping between keys and their minimal perfect hash.
	// For a given key 'i' in the list provided to New(),
	// Map[i] is the minimal hash index for key[i].
	// Note: the minimal hash index is 1 based (NOT zero based).
	Map []uint64
}

// Gamma is an expansion factor for each of the bitvectors we build.
// Empirically, 2.0 is found to be a good balance between speed and
// space usage. See paper for more details.
const Gamma float64 = 2.0

// Maximum number of attempts (level) at making a perfect hash function.
// Per the paper, each successive level exponentially reduces the
// probability of collision.
const MaxLevel uint = 200

// New creates a new minimal hash function to represent the keys in 'keys'.
// Upon successful return from this function, the Map element of BBHash will
// be appropriately populated.
func New(g float64, keys []uint64) (*BBHash, error) {
	var lvl uint

	sz := uint(len(keys))
	A := NewBitVector(sz, g)
	coll := NewBitVector(sz, g)
	redo := make([]uint64, 0, sz)
	salt := rand64()
	okey := keys

	bb := &BBHash{
		salt: salt,
	}

	for len(keys) > 0 {
		for _, k := range keys {
			i := hash(k, salt, lvl) % A.Size()

			if coll.IsSet(i) {
				continue
			}
			if A.IsSet(i) {
				coll.Set(i)
				continue
			}
			A.Set(i)
		}

		// Sadly, no way to avoid scanning the keyspace _twice_.
		A.Reset()
		for _, k := range keys {
			i := hash(k, salt, lvl) % A.Size()

			if coll.IsSet(i) {
				redo = append(redo, k)
				continue
			}
			A.Set(i)
		}

		bb.bits = append(bb.bits, A)

		keys = redo
		if len(keys) == 0 {
			break
		}

		redo = redo[:0]
		sz = uint(len(keys))
		A = NewBitVector(sz, g)
		coll.Reset()
		lvl++

		if lvl > MaxLevel {
			return nil, fmt.Errorf("can't find minimal perf hash after %d tries", lvl)
		}
	}

	bb.preComputeRank()

	// We reuse 'redo' to return the final index of hash keys
	bb.Map = redo[:0]
	for i, k := range okey {
		j := bb.Find(k)
		if j == 0 {
			s := fmt.Sprintf("can't find key %#x at %d", k, i)
			panic(s)
		}
		bb.Map = append(bb.Map, j)

	}

	return bb, nil
}

// Find returns a unique integer representing the minimal hash for key 'k'.
// The return value is meaningful ONLY for keys in the original key set (provided 
// at the time of construction of the minimal-hash).
// If the key is in the original key-set
func (bb *BBHash) Find(k uint64) uint64 {
	for lvl, bv := range bb.bits {
		i := hash(k, bb.salt, uint(lvl)) % bv.Size()

		if !bv.IsSet(i) {
			continue
		}

		rank := 1 + bb.ranks[lvl] + bv.Rank(i)
		return rank
	}

	return 0
}

// Precompute ranks for each level so we can answer queries quickly.
func (bb *BBHash) preComputeRank() {
	var pop uint64
	bb.ranks = make([]uint64, len(bb.bits))

	// We omit the first level in rank calculation; this avoids a special
	// case in Find() when we are looking at elements in level-0.
	for l, bv := range bb.bits {
		bb.ranks[l] = pop
		pop += bv.ComputeRank()
	}
}

// One round of Zi Long Tan's superfast hash
func hash(key, salt uint64, lvl uint) uint64 {
	const m uint64 = 0x880355f21e6d1965
	var h uint64 = m

	h ^= mix(key)
	h >>= lvl
	h *= m
	return mix(h) ^ salt
}

// compression function for fasthash
func mix(h uint64) uint64 {
	h ^= h >> 23
	h *= 0x2127599bf4325c37
	h ^= h >> 47
	return h
}

func rand64() uint64 {
	var b [8]byte

	n, err := rand.Read(b[:])
	if err != nil || n != 8 {
		panic("rand read failure")
	}
	return binary.BigEndian.Uint64(b[:])
}
